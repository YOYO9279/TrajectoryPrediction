import geohash2
import grequests
import grequests_throttle as gt
import pandas as pd
from pyspark.sql import SparkSession
from tqdm import tqdm

from conf.config import s, mysqlConn, CROSSING_SINK_TABLE
from utils.db.df_insert_ignore import save_dataframe
from utils.geo.coordTransform_utils import gcj02_to_wgs84
from utils.pusher.wx import wx_reminder

SOURCE_TABLE = 'spark.ods_track_rk_geohash'

CROSSING_SINK_TABLE = "crossing_geohash"


def sinkCrossing():
    df = spark.sql(
        f'SELECT  map_longitude,map_latitude from {SOURCE_TABLE} LIMIT 250000 ')

    df = df.toPandas()

    spark.stop()

    urls = [
        f'https://restapi.amap.com/v3/geocode/regeo?location={r["map_longitude"]},{r["map_latitude"]}&key=30a423f69a6cb59baef9f2f55ce64c41&radius=3000&extensions=all'
        for index, r in df.iterrows()]

    reqs = [grequests.get(url, session=s) for url in urls]
    map_long_list = []
    map_lat_list = []
    gps_long_list = []
    gps_lat_list = []
    for i in tqdm(gt.map(reqs, rate=150)):
        try:
            if i is not None:
                map_long = float(str(i.json()["regeocode"]["roadinters"][0]["location"]).split(",")[0])
                map_lat = float(str(i.json()["regeocode"]["roadinters"][0]["location"]).split(",")[1])
                gps_long = gcj02_to_wgs84(map_long, map_lat)[0]
                gps_lat = gcj02_to_wgs84(map_long, map_lat)[1]

                map_long_list.append(map_long)
                map_lat_list.append(map_lat)
                gps_long_list.append(gps_long)
                gps_lat_list.append(gps_lat)
        except Exception as e:
            print(e)

    geohash_list = [geohash2.encode(lat, long) for lat, long in zip(map_lat_list, map_long_list)]

    crossing_data = {'map_longitude': map_long_list, 'map_latitude': map_lat_list, 'gps_longitude': gps_long_list,
                     'gps_latitude': gps_lat_list, 'geohash': geohash_list}

    crossing = pd.DataFrame(crossing_data)
    print(crossing)
    save_dataframe(mysqlConn, crossing, "crossing_geohash")

    print("SinkCrossing Done")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("sink Crossing").master("yarn").enableHiveSupport().getOrCreate()

    sinkCrossing()

    wx_reminder()

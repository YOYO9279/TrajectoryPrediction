import time
import pandas
import requests
import json
from pyspark.sql import SparkSession
from tqdm import tqdm

from conf.config import *
from utils.db.df_insert_ignore import save_dataframe
from utils.db.getConn import getMysqlConn
from utils.geo.coordTransform_utils import gcj02_to_wgs84




# 输入
# original_table


# 输出 mysql 路口表
# 先建好表
# create table spark.`area02`
# (
#     id            int auto_increment
#         primary key,
#     map_longitude double null,
#     map_latitude  double null,
#     gps_longitude double null,
#     gps_latitude  double null,
#     constraint `map_longitude_map_latitude_uindex`
#         unique (map_longitude, map_latitude)
# );


mysql_conn = getMysqlConn()


s = requests.session()

GDPOSTURL = 'https://restapi.amap.com/v3/geocode/regeo?location={},{}&key=30a423f69a6cb59baef9f2f55ce64c41&radius=3000&extensions=all'


# TODO 做并发请求

def getCrossing(x, y):
    u = GDPOSTURL.format(x, y)
    while True:
        try:
            text = s.get(u, timeout=300).text
            roadinters = json.loads(text)["regeocode"]["roadinters"]
            for i in roadinters:
                location = str(i["location"]).split(",")
                map_long_list.append(float(location[0]))
                map_lat_list.append(float(location[1]))

                conver_location = gcj02_to_wgs84(float(location[0]), float(location[1]))
                gps_long_list.append(conver_location[0])
                gps_lat_list.append(conver_location[1])
            return
        except Exception as e:
            time.sleep(1)
            print("retry")


if __name__ == '__main__':

    spark = SparkSession.builder.appName("getCrossing").master("yarn").enableHiveSupport().getOrCreate()

    gps_long_list = []
    gps_lat_list = []
    map_long_list = []
    map_lat_list = []

    # 将 long la去重
    df = spark.sql(
        f'SELECT DISTINCT map_longitude,map_latitude from {original_table}  LIMIT 2000')

    filterDF = df.toPandas()
    for index, r in tqdm(filterDF.iterrows(), total=filterDF.shape[0], desc="[POST] GD API"):
        getCrossing(r["map_longitude"], r["map_latitude"])

    crossing_data = {'map_longitude': map_long_list,
                     'map_latitude': map_lat_list,
                     'gps_longitude': gps_long_list,
                     'gps_latitude': gps_lat_list}

    cross = pandas.DataFrame(crossing_data)
    save_dataframe(mysql_conn, cross, config["table"]["mysql"]["coor_table"])
    spark.stop()
    print("GetCrossing Done")

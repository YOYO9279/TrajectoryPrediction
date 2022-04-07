from math import ceil

import geohash2
import grequests
import pandas as pd
from pandas import DataFrame
from pyspark.sql import SparkSession

from conf.config import *
from utils.geo.coordTransform_utils import gcj02_to_wgs84
from utils.req.concur_request import concurQ

SINK_TABLE = "spark.ods_track_geohash_makeup"


def getMorePoints(counts, tid, trid):
    page = ceil(counts / 999)
    urls = [
        f"https://tsapi.amap.com/v1/track/terminal/trsearch?key={key}&sid={sid}&tid={tid}&trid={trid}&recoup=1&gap=50&pagesize=999&page={i + 2}"
        for i in range(page - 1)]
    reqs = [grequests.get(url, session=s) for url in urls]
    resp = concurQ(reqs)
    points = [j for i in resp for j in i.json()["data"]["tracks"][0]["points"]]

    return points


def sinkPoints():
    car_infoDF = pd.read_sql_query(f'select * from {CARINFO_TABLE} WHERE id < 1000', con=mysqlConn)

    tidL = []
    tridL = []
    car_numL = []
    gps_longitudeL = []
    gps_latitudeL = []
    gps_timeL = []
    map_longitudeL = []
    map_latitudeL = []

    oritidL = [r['tid'] for index, r in car_infoDF.iterrows()]
    oritridL = [r['trid'] for index, r in car_infoDF.iterrows()]
    oricar_numL = [r['car_number'] for index, r in car_infoDF.iterrows()]
    urls = [
        f"https://tsapi.amap.com/v1/track/terminal/trsearch?key={key}&sid={sid}&tid={r['tid']}&trid={r['trid']}&recoup=1&gap=50&pagesize=999&page=1"
        for index, r in car_infoDF.iterrows()]

    reqs = [grequests.get(url, session=s) for url in urls]

    n = 0
    for resp in concurQ(reqs):
        points = []
        points += resp.json()["data"]["tracks"][0]["points"]
        counts = resp.json()["data"]["tracks"][0]["counts"]
        if counts > 999:
            points += getMorePoints(counts, oritidL[n], oritridL[n])
        map_longitudeL += [float(str(i["location"]).split(",")[0]) for i in points]
        map_latitudeL += [float(str(i["location"]).split(",")[1]) for i in points]
        gps_longitudeL += [
            gcj02_to_wgs84(float(str(i["location"]).split(",")[0]), float(str(i["location"]).split(",")[1]))[0] for i in
            points]
        gps_latitudeL += [
            gcj02_to_wgs84(float(str(i["location"]).split(",")[0]), float(str(i["location"]).split(",")[1]))[1] for i in
            points]

        tidL += [oritidL[n] for x in range(len(points))]
        tridL += [oritridL[n] for x in range(len(points))]
        car_numL += [oricar_numL[n] for x in range(len(points))]
        gps_timeL += [x for x in range(len(points))]
        n = n + 1

    geohash_list = [geohash2.encode(lat, long) for lat, long in zip(map_latitudeL, map_longitudeL)]

    res = {"map_longitude": map_longitudeL,
           "map_latitude": map_latitudeL,
           "gps_longitude": gps_longitudeL,
           "gps_latitude": gps_latitudeL,
           "car_number": car_numL,
           "geohash": geohash_list,
           "tid": tidL,
           "trid": tridL,
           "gps_time": gps_timeL}
    resDF = DataFrame(res)
    print(resDF)
    spark.createDataFrame(resDF).write.mode("overwrite").saveAsTable(SINK_TABLE)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("sinkPoint").master("yarn").enableHiveSupport().getOrCreate()

    sinkPoints()

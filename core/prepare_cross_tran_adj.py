import json

import grequests
import pandas as pd
from geopy.distance import geodesic
from pyspark.sql import SparkSession
from utils.db.df_insert_ignore import save_dataframe
from utils.geo.coordTransform_utils import gcj02_to_wgs84
from conf.config import *
import grequests_throttle as gt


def sinkCrossing():
    df = spark.sql(
        f'SELECT DISTINCT map_longitude,map_latitude from {SOURCE_TABLE} limit 10')

    df = df.toPandas()

    urls = [
        f'https://restapi.amap.com/v3/geocode/regeo?location={r["map_longitude"]},{r["map_latitude"]}&key=30a423f69a6cb59baef9f2f55ce64c41&radius=3000&extensions=all'
        for index, r in df.iterrows()]

    print(urls)
    reqs = [grequests.get(url, session=s) for url in urls]
    map_long_list = [
        float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[0]) for i in
        gt.map(reqs, rate=50)]

    map_lat_list = [
        float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[1]) for i in
        gt.map(reqs, rate=50)]
    gps_long_list = [
        gcj02_to_wgs84(float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[0]),
                       float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[1]))[0] for i
        in gt.map(reqs, rate=50)]

    gps_lat_list = [
        gcj02_to_wgs84(float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[0]),
                       float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[1]))[1] for i
        in gt.map(reqs, rate=50)]

    crossing_data = {'map_longitude': map_long_list, 'map_latitude': map_lat_list, 'gps_longitude': gps_long_list,
                     'gps_latitude': gps_lat_list}

    crossing = pd.DataFrame(crossing_data)
    print(crossing)
    save_dataframe(mysqlConn, crossing, CROSSING_SINK_TABLE)

    print("SinkCrossing Done")


def SinkTransfer():
    CROSSING_TEMP_VIEW = "CROSSINGTEMPVIEW"

    df = pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)
    spark.createDataFrame(df).createOrReplaceTempView(CROSSING_TEMP_VIEW)
    spark.sql(f'''
    select   a.car_number as car_number,
             a.gps_time   as gps_time,
             b.id         as id
        from {SOURCE_TABLE} a
               cross join {CROSSING_TEMP_VIEW} b
                          on dist(a.gps_latitude, a.gps_longitude, b.gps_latitude, b.gps_longitude) < {CROSSING_DISTANCE}
    ''').repartition(100, "car_number").cache().createOrReplaceTempView("coor_car")

    print("coor_car done")

    fromtoDF = spark.sql(
        f'''
            select c_from_id as from_id, c_to_id as to_id, count(1) as cnt
            from (select a.id as c_from_id, b.id as c_to_id
                  from coor_car a
                           cross join coor_car b
                                      on a.gps_time < b.gps_time and a.car_number = b.car_number) c
            group by from_id, to_id
    ''')

    fromtoDF.toPandas().to_sql(TRANSFER_SINK_TABLE, clickhouseConn, if_exists='append', index=False)
    print("SinkTransfer Done")


def SinkAdjacent():
    spark.createDataFrame(
        pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)).createOrReplaceTempView(
        CROSSING_SINK_TABLE)

    curnextDF = spark.sql(
        f'''
                SELECT *
                from (SELECT DISTINCT id                                                               as cur,
                                      lead(id, 1, -1) over (PARTITION BY car_number ORDER BY gps_time) as next
                      from coor_car)
                where cur != next and next != -1
                ''')
    curnextDF.toPandas().to_sql(ADJACENT_SINK_TABLE, clickhouseConn, if_exists='append', index=False)
    print("SinkAdjacent Done")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("preCrosTranAdj").master("yarn").enableHiveSupport().getOrCreate()
    spark.udf.register("dist", lambda x1, y1, x2, y2: geodesic((x1, y1), (x2, y2)).m)

    sinkCrossing()

    SinkTransfer()

    SinkAdjacent()

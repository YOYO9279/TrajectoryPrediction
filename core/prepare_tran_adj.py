import json

import grequests
import pandas as pd
from geopy.distance import geodesic
from pyspark.sql import SparkSession
from utils.db.df_insert_ignore import save_dataframe
from utils.geo.coordTransform_utils import gcj02_to_wgs84
from conf.config import *
import grequests_throttle as gt



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
    spark = SparkSession.builder.appName("get TranAdj").master("yarn").enableHiveSupport().getOrCreate()
    spark.udf.register("dist", lambda x1, y1, x2, y2: geodesic((x1, y1), (x2, y2)).m)


    SinkTransfer()

    SinkAdjacent()

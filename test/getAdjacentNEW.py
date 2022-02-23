import json
import time

import pandas as pd
import requests
from geopy.distance import geodesic
from pyspark.sql import SparkSession
from tqdm import tqdm

from conf.config import *
from core.getCrossing import mysql_conn
from utils.db.df_insert_ignore import save_dataframe


def dist(a, b):
    return geodesic(a, b).m


# 输入
# "cross100"

# coor_car_table

# 输出
# adj_table
curL = []
nextL = []
# 中间表
cur_nextTable = 'cur_next'

if __name__ == '__main__':
    spark = SparkSession.builder.appName("getAdjacent").master("yarn").enableHiveSupport().getOrCreate()
    spark.udf.register("dist", lambda x1, y1, x2, y2: dist((x1, y1), (x2, y2)))

    ft = spark.sql(
        f'''
            SELECT *
            from (SELECT DISTINCT id                                                               as cur,
                                  lead(id, 1, -1) over (PARTITION BY car_number ORDER BY gps_time) as next
                  from (SELECT a.car_number as car_number,
                               a.gps_time   as gps_time,
                               b.id         as id
                        from spark.demo100 a
                                 cross join spark.cross100 b
                                            on dist(a.gps_latitude, a.gps_longitude, b.gps_latitude, b.gps_longitude) < 20
                       ))
            where cur != next
              and next != -1
        ''').toPandas()

    for index, row in tqdm(ft.iterrows(), total=ft.shape[0]):
        curL.append(int(row['cur']))
        nextL.append(int(row['next']))

    adj = {
        "a_id": curL,
        "b_id": nextL,
    }

    pd.DataFrame(adj).to_sql("adj100", con=mysql_conn, index=False, if_exists='replace')

    spark.stop()

    print("getAdjacent Done")

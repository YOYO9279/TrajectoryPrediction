import json
import time

import pandas as pd
import requests
from pyspark.sql import SparkSession

from conf.config import *
from core.getCrossing import mysql_conn
from utils.db.df_insert_ignore import save_dataframe


# 输入
# "cross100"

# coor_car_table

# 输出
# adj_table

# 中间表
cur_nextTable = 'cur_next'

s = requests.session()
url = 'https://restapi.amap.com/v3/direction/driving?origin={},{}&destination={},{}&extensions=all&output=json&key=30a423f69a6cb59baef9f2f55ce64c41'


def isAdjacent(x1, y1, x2, y2):
    u = url.format(x1, y1, x2, y2)
    while True:
        try:
            text = s.get(u).text
            steps = json.loads(text)['route']['paths'][0]['steps']
            return 1 if len(steps) == 1 else 0
        except Exception as e:
            time.sleep(1)
            print("retry")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("getAdjacent").master("yarn").enableHiveSupport().getOrCreate()

    spark.udf.register("isAdj_udf", lambda x1, y1, x2, y2: isAdjacent(x1, y1, x2, y2))


    df = pd.read_sql_query(f'select * from spark.cross100', con=mysql_conn)

    spark.createDataFrame(df).createOrReplaceTempView("cross100")

    df2 = spark.sql(
        f'''
                select *
        from (SELECT a.id                                                                        as a_id,
                     a.map_longitude                                                             as a_map_longitude,
                     a.map_latitude                                                              as a_map_latitude,
                     a.gps_longitude                                                             as a_gps_longitude,
                     a.gps_latitude                                                              as a_gps_latitude,
                     b.id                                                                        as b_id,
                     b.map_longitude                                                             as b_map_longitude,
                     b.map_latitude                                                              as b_map_latitude,
                     b.gps_longitude                                                             as b_gps_longitude,
                     b.gps_latitude                                                              as b_gps_latitude,
                     isAdj_udf(a.map_longitude, a.map_latitude, b.map_longitude, b.map_latitude) as isAdj
              FROM cross100 a CROSS JOIN cross100 b
              ON a.id < b.id
              ORDER BY a_id, b_id)
        where isAdj = 1
        ''')




    isAdjDF = df2.toPandas()
    save_dataframe(mysql_conn, isAdjDF, "adj100")
    # isAdjDF.to_sql(isAdjTable, mysql_conn, index=False)
    spark.stop()

    print("getAdjacent Done")

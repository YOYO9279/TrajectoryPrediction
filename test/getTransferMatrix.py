from pyspark.sql import SparkSession
import pandas as pd
from geopy.distance import geodesic

from conf.config import *
from core.getCrossing import mysql_conn


# # 输入
#
# original_table
# coor_table
#
# # 输出
#
# tran_table
#

# 中间表
# coor_car_table


def getCoor():
    df = pd.read_sql_query(f'select * from spark.cross100', con=mysql_conn)
    return spark.createDataFrame(df)


def dist(a, b):
    return geodesic(a, b).m


if __name__ == '__main__':
    spark = SparkSession.builder.appName("getTransfer").master("yarn").enableHiveSupport().getOrCreate()
    df1 = getCoor()
    df1.createOrReplaceTempView("cross100")
    spark.udf.register("dist", lambda x1, y1, x2, y2: dist((x1, y1), (x2, y2)))

    df1.show()


    df3 = spark.sql(
        f'''
        with coor_car as (select a.car_number as car_number,
                                 a.gps_time   as gps_time,
                                 b.id         as id
                          from spark.demo100 a
                                   cross join cross100 b
                                              on dist(a.gps_latitude, a.gps_longitude, b.gps_latitude, b.gps_longitude) < 20)
        select c_from_id as from_id, c_to_id as to_id, count(1) as cnt
        from (select a.id as c_from_id, b.id as c_to_id
              from coor_car a
                       cross join coor_car b
                                  on a.gps_time < b.gps_time and a.car_number = b.car_number) c
        group by from_id, to_id
''')

    df3.show()
    ## TODO 这里是repalce
    df3.toPandas().to_sql("tran100", con=mysql_conn, index=False, if_exists='replace')

    spark.stop()

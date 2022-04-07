import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from conf.config import *

SOURCE_TABLE = "spark.ods_track_rk_geohash"



CROSSING_TEMP_VIEW = "CROSSINGTEMPVIEW"
def SinkTransfer():

    df = pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)
    spark.createDataFrame(df).createOrReplaceTempView(CROSSING_TEMP_VIEW)
    spark.sql(f'''
    select   a.car_number as car_number,
             a.gps_time   as gps_time,
             b.id         as id
        from {SOURCE_TABLE} a
               cross join {CROSSING_TEMP_VIEW} b
                          on substr(a.geohash,1,8) == substr(b.geohash,1,8)   
    ''').cache().createOrReplaceTempView("coor_car")

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

    curnextDF = spark.sql(
        f'''
                SELECT *
                from (SELECT          id                                                               as cur,
                                      lead(id, 1, -1) over (PARTITION BY car_number ORDER BY gps_time) as next
                      from coor_car)
                where cur != next and next != -1
                ''')
    curnextDF.toPandas().to_sql(ADJACENT_SINK_TABLE, clickhouseConn, if_exists='append', index=False)
    print("SinkAdjacent Done")


def save_A_Z():
    global A, A2, A3, Z

    transferDF = pd.read_sql_query(f'select * from {TRANSFER_SINK_TABLE}', con=clickhouseConn)
    crossingDF = pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)
    adjacentDF = pd.read_sql_query(f'select * from {ADJACENT_SINK_TABLE}', con=clickhouseConn)

    m = crossingDF.last_valid_index() + 2
    A = np.mat(np.zeros((m, m)))
    Z = np.mat(np.zeros((m, m)))

    cnt = 0
    for index, row in transferDF.iterrows():
        A[int(row['from_id']), int(row['to_id'])] = row['cnt']
        cnt += row['cnt']

    A = A / cnt
    A2 = A.dot(A)
    A3 = A2.dot(A)

    for index, row in adjacentDF.iterrows():
        Z[int(row['cur']), int(row['next'])] = 1
        Z[int(row['next']), int(row['cur'])] = 1

    print("save ...")
    np.save(A_PATH, A)
    np.save(A2_PATH, A2)
    np.save(A3_PATH, A3)
    np.save(Z_PATH, Z)


    print("prepare A Z done")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("get TranAdj").master("yarn").enableHiveSupport().getOrCreate()
    #
    # SinkTransfer()
    #
    # SinkAdjacent()

    save_A_Z()

#!/root/anaconda3/envs/pyspark/bin/python
# -*- coding: UTF-8 -*-


# a+b+c=1  a>b>c    aTA+bTA^2+cTA^3
# 1、将转移矩阵读出来 算出A A^2 A^3
# 2、所有车的路径读出来
from collections import defaultdict

from geopy.distance import geodesic
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
from tqdm import tqdm

from core import mysql_con
from conf.config import *

m = 0


def dist(a, b):
    return geodesic(a, b).m


def getA():
    df = pd.read_sql_query(f'select * from {tran_table}', con=mysql_con)
    df_coor = pd.read_sql_query(f'select id from {coor_table}', con=mysql_con)
    global m
    m = df_coor.last_valid_index() + 2

    A = np.mat(np.zeros((m, m)))
    cnt = 0
    for index, row in df.iterrows():
        A[int(row['from_id']), int(row['to_id'])] = row['cnt']
        cnt += row['cnt']

    return A / cnt


def getZ():
    df = pd.read_sql_query(f'select * from {adj_table}', con=mysql_con)
    Z = np.mat(np.zeros((m, m)))

    for index, row in df.iterrows():
        Z[int(row['a_id']), int(row['b_id'])] = 1
        Z[int(row['b_id']), int(row['a_id'])] = 1

    return Z


def getFromTo():
    df = pd.read_sql(f'select * from {coor_table}', con=mysql_con)
    df = spark.createDataFrame(df)

    df.createOrReplaceTempView(coor_table)

    df2 = spark.sql(
        f"select a.*,b.id from (SELECT map_longitude, map_latitude, gps_time, gps_speed, direction, event, alarm_code, gps_longitude, gps_latitude, altitude, tachometer_speed, miles, error_type, access_code, receiving_time, color, province, car_number, dt from (SELECT *,row_number() over(PARTITION BY gps_longitude, gps_latitude ORDER BY province) rank from {original_table}  ) m WHERE m.rank=1) a cross join {coor_table} b on dist(a.gps_latitude,a.gps_longitude,b.gps_latitude,b.gps_longitude) < 20")

    df2.show()

    df2.createTempView(coor_car_table)

    fromTodf = spark.sql(
        f"SELECT id as cur, lag(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre1,lag(id,2,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre2,lag(id,3,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre3,lead(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as next from {coor_car_table}")

    return fromTodf.toPandas()


def statistics(ft):
    Y = 0
    N = 0
    T = defaultdict(int)

    for index, row in tqdm(ft.iterrows(), total=ft.shape[0]):

        S = A[row['pre1']] * 0.6 + A2[row['pre2']] * 0.3 + A3[row['pre3']] * 0.1

        print("S: ", S)
        print("Z: ", Z[row['cur']])
        S = np.multiply(S, Z[row['cur']])

        prediect = np.argmax(S, axis=1)

        if row['pre1'] != -1 or row['pre2'] != -1 or row['pre3'] != -1 or row['next'] != -1:
            # print("max :", S.max(axis=1))
            # print("cur :", row['cur'])
            # print("next : ", row['next'])
            # print("predict :", prediect)

            T[int(prediect[0])] += 1

            if prediect == row['next']:
                Y += 1
            else:
                N += 1
    print(T)
    print("Y:" + str(Y))
    print("N:" + str(N))
    print("correct:" + str(Y / (Y + N)))


if __name__ == '__main__':
    # 1、将转移矩阵读出来 算出A A^2 A^3
    np.set_printoptions(threshold=np.inf)

    A = getA()
    A2 = A.dot(A)
    A3 = A2.dot(A)

    Z = getZ()

    print("A:", A)
    print("Z:", Z)

    # # 2、所有车的路径读出来
    # spark = SparkSession.builder.appName("getABC").master("yarn").enableHiveSupport().getOrCreate()
    # spark.udf.register("dist", lambda x1, y1, x2, y2: dist((x1, y1), (x2, y2)))
    #
    # ft = getFromTo()
    # # ft = spark.sql("SELECT id as cur, lag(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre1,lag(id,2,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre2,lag(id,3,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre3,lead(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as next from `spark`.`06051d`").toPandas()
    # statistics(ft)

    # spark.stop()

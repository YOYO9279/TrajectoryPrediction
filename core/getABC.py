#!/root/anaconda3/envs/pyspark/bin/python
# -*- coding: UTF-8 -*-


# a+b+c=1  a>b>c    aTA+bTA^2+cTA^3
# 1、将转移矩阵读出来 算出A A^2 A^3
# 2、所有车的路径读出来
from collections import defaultdict

from pyspark.sql import SparkSession
import numpy as np
import MySQLdb
import pandas as pd
from tqdm import tqdm

mysql_con = MySQLdb.connect(host='node01',
                            port=3306, user='root', passwd='123456',
                            db='spark')



def getA():
    df = pd.read_sql('select * from tranMatrix', con=mysql_con)
    A = np.mat(np.zeros((342, 342)))
    cnt = 0
    for index, row in df.iterrows():
        A[int(row['c1_id']), int(row['c2_id'])] = row['cnt']
        cnt += row['cnt']

    return A / cnt


def getFromTo():
    df = pd.read_sql('select * from mysql_area01_coordinate', con=mysql_con)
    df = spark.createDataFrame(df)

    df.createOrReplaceTempView("coor")


    df2 = spark.sql(
        "select * from spark.area02 inner join coor c where sqrt((c.x-area02.gps_longitude)*(c.x-area02.gps_longitude)+(c.y-area02.gps_latitude)*(c.y-area02.gps_latitude)) < 0.0001")
    df2.createTempView("coor_car")

    fromTodf = spark.sql(
        "SELECT id as cur, lag(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre1,lag(id,2,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre2,lag(id,3,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre3,lead(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as next from coor_car")

    return fromTodf.toPandas()


def statistics(ft):
    Y = 0
    N = 0
    T = defaultdict(int)

    for index, row in tqdm(ft.iterrows(),total=ft.shape[0]):

        S = A[row['pre1']] * 0.8 + A2[row['pre2']] * 0.1 + A3[row['pre3']] * 0.1

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
    A2 = A * A
    A3 = A2 * A
    print(A[:,55].max(3))

    # 2、所有车的路径读出来

    spark = SparkSession.builder.appName("getABC").master("yarn").enableHiveSupport().getOrCreate()

    ft = getFromTo()
    # ft = spark.sql("SELECT id as cur, lag(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre1,lag(id,2,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre2,lag(id,3,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre3,lead(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as next from `spark`.`06051d`").toPandas()
    statistics(ft)

    # spark.stop()

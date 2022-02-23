#!/root/anaconda3/envs/pyspark/bin/python
# -*- coding: UTF-8 -*-


# a+b+c=1  a>b>c    aTA+bTA^2+cTA^3
# 1、将转移矩阵读出来 算出A A^2 A^3
# 2、所有车的路径读出来

from geopy.distance import geodesic
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
from tqdm import tqdm

from core import mysql_con

m = 0

curL = []
pre1L = []
pre2L = []
pre3L = []
nextL = []
predictL = []


def dist(a, b):
    return geodesic(a, b).m


def getA():
    df = pd.read_sql_query(f'select * from spark.tran100', con=mysql_con)
    df_coor = pd.read_sql_query(f'select * from spark.cross100', con=mysql_con)
    m = df_coor.last_valid_index() + 2
    print(df_coor)
    print(m)
    A = np.mat(np.zeros((m, m)))
    cnt = 0
    for index, row in df.iterrows():
        A[int(row['from_id']), int(row['to_id'])] = row['cnt']
        cnt += row['cnt']

    return A / cnt


def getZ():
    df = pd.read_sql_query(f'select * from spark.adj100', con=mysql_con)
    Z = np.mat(np.zeros((m, m)))

    for index, row in df.iterrows():
        Z[int(row['a_id']), int(row['b_id'])] = 1
        Z[int(row['b_id']), int(row['a_id'])] = 1

    return Z


def getFromTo():
    fromTodf = spark.sql(
        f'''
            select *
            from (SELECT id                                                               as cur,
                         lag(id, 1, -1) over (PARTITION BY car_number ORDER BY gps_time)  as pre1,
                         lag(id, 2, -1) over (PARTITION BY car_number ORDER BY gps_time)  as pre2,
                         lag(id, 3, -1) over (PARTITION BY car_number ORDER BY gps_time)  as pre3,
                         lead(id, 1, -1) over (PARTITION BY car_number ORDER BY gps_time) as next
                  from (SELECT a.car_number as car_number,
                               a.gps_time   as gps_time,
                               b.id         as id
                        from spark.demo100 a
                                 cross join spark.cross100 b
                                            on dist(a.gps_latitude, a.gps_longitude, b.gps_latitude, b.gps_longitude) < 20
                       ))
            where cur != next
              and pre1 != pre2
              and pre2 != pre3
              and pre3 != cur
''')

    fromTodf.show()
    return fromTodf.toPandas()


def statistics(ft):
    Y = 0
    N = 0
    for index, row in tqdm(ft.iterrows(), total=ft.shape[0]):

        S = A[row['pre1']] * 0.6 + A2[row['pre2']] * 0.3 + A3[row['pre3']] * 0.1

        S = np.multiply(S, Z[row['cur']])

        predict = np.argmax(S, axis=1)

        if int(row['pre1']) != -1 and int(row['pre2']) != -1 and int(row['pre3']) != -1 and int(
                row['next']) != -1 and int(predict[0]) != 0:
            pre1L.append(int(row['pre1']))
            pre2L.append(int(row['pre2']))
            pre3L.append(int(row['pre3']))
            curL.append(int(row['cur']))
            nextL.append(int(row['next']))
            predictL.append(int(predict[0]))

            if int(predict[0]) == int(row['next']):
                Y += 1
            else:
                N += 1

    res = {"pre1": pre1L,
           "pre2": pre2L,
           "pre3": pre3L,
           "cur": curL,
           "next": nextL,
           "predict": predictL
           }

    pd.DataFrame(res).to_csv('predict.csv')

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
    spark = SparkSession.builder.appName("getABC").master("yarn").enableHiveSupport().getOrCreate()
    spark.udf.register("dist", lambda x1, y1, x2, y2: dist((x1, y1), (x2, y2)))

    ft = getFromTo()
    # ft = spark.sql("SELECT id as cur, lag(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre1,lag(id,2,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre2,lag(id,3,-1) over(PARTITION BY car_number ORDER BY gps_time) as pre3,lead(id,1,-1) over(PARTITION BY car_number ORDER BY gps_time) as next from `spark`.`06051d`").toPandas()
    statistics(ft)

    spark.stop()

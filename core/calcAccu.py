import time
import numpy as np
import pandas as pd
from geopy.distance import geodesic
from pyspark.sql import SparkSession
from tqdm import tqdm

from etc.config import *


def PrepareData():
    global A, A2, A3, Z
    transferDF = pd.read_sql_query(f'select * from {TRANSFER_SINK_TABLE}', con=clickhouseConn)
    crossingDF = pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)
    adjacentDF = pd.read_sql_query(f'select * from {ADJACENT_SINK_TABLE}', con=clickhouseConn)
    spark.createDataFrame(crossingDF).createOrReplaceTempView(CROSSING_SINK_TABLE)

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


def getPrecurnextDF(i):
    spark.sql(f"""

                SELECT a.car_number as car_number,
                            a.gps_time   as gps_time,
                           b.id         as id
                    from (select * from {SOURCE_TABLE} order by car_number limit {i}) a
                             cross join {CROSSING_SINK_TABLE} b
                   on dist(a.gps_latitude, a.gps_longitude, b.gps_latitude, b.gps_longitude) < {CROSSING_DISTANCE}
    """).createOrReplaceTempView("tt")

    precurnextDF = spark.sql(
        f'''
            SELECT *
            FROM
              (SELECT id AS cur,
                      lag(id, 1, -1) over (PARTITION BY car_number
                                           ORDER BY gps_time) AS pre1,
                      lag(id, 2, -1) over (PARTITION BY car_number
                                           ORDER BY gps_time) AS pre2,
                      lag(id, 3, -1) over (PARTITION BY car_number
                                           ORDER BY gps_time) AS pre3,
                      lead(id, 1, -1) over (PARTITION BY car_number
                                            ORDER BY gps_time) AS next
               FROM
                 (SELECT id,
                         car_number,
                         gps_time
                  FROM
                    (SELECT lag(id, 1, -1) over (PARTITION BY car_number
                                                 ORDER BY gps_time) AS p1,
                            id,
                            car_number,
                            gps_time
                     FROM tt) c
                  WHERE p1!=id ) d)e
            WHERE pre1!=-1
              AND pre2!=-1
              AND pre3!=-1
              AND next!=-1
    ''')
    return precurnextDF





def DoCalc(precurnextDF, i, a, b, c):
    curL = []
    pre1L = []
    pre2L = []
    pre3L = []
    nextL = []
    predictL = []
    Y = 0
    N = 0
    for index, row in tqdm(precurnextDF.iterrows(), total=precurnextDF.shape[0]):

        S = A[row['pre1']] * 0.6 + A2[row['pre2']] * 0.3 + A3[row['pre3']] * 0.1

        S = np.multiply(S, Z[row['cur']])

        predict = np.argmax(S, axis=1)

        if int(predict[0]) != 0:
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

    curtime = str(time.strftime("%Y%m%d_%H%M%S", time.localtime()))
    correct = str(round(Y / (Y + N) * 100, 2)) + "%"
    saveName = "_".join([curtime, correct, str(i), str(a), str(b), str(c)]) + ".csv"
    pd.DataFrame(res).to_csv("../res/calc/"+saveName)

    print("Y:" + str(Y))
    print("N:" + str(N))
    print("correct:" + correct)


if __name__ == '__main__':

    spark = SparkSession.builder.appName("calcAccu").master("yarn").enableHiveSupport().getOrCreate()
    spark.udf.register("dist", lambda x1, y1, x2, y2: geodesic((x1, y1), (x2, y2)).m)

    PrepareData()
    size = [10000]
    a, b, c = 0.5, 0.3, 0.2
    for i in size:
        precurnextDF = getPrecurnextDF(i)
        DoCalc(precurnextDF.toPandas(), i, a, b, c)

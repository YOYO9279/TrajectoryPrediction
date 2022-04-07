import time
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from tqdm import tqdm

from conf.config import *
from utils.pusher.wx import wx_reminder

np.set_printoptions(threshold=np.inf)


def get_data_from_local():
    global A, A2, A3, Z

    A = np.load(A_PATH+".npy")
    A2 = np.load(A2_PATH+".npy")
    A3 = np.load(A3_PATH+".npy")
    Z = np.load(Z_PATH+".npy")
    crossingDF = pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)
    spark.createDataFrame(crossingDF).createOrReplaceTempView(CROSSING_SINK_TABLE)
    print("get data done")



def getPrecurnextDF(i):
    spark.sql(f"""

                SELECT a.car_number as car_number,
                            a.gps_time   as gps_time,
                           b.id         as id
                    from (select * from {CALC_SOURCE_TABLE} WHERE dt = '2018-10-08'limit {i}) a
                             cross join {CROSSING_SINK_TABLE} b
                   on substr(a.geohash,1,8) == substr(b.geohash,1,8)
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

        S = A[row['pre1']] * a + A2[row['pre2']] * b + A3[row['pre3']] * c

        S = np.multiply(S, Z[row['cur']])

        predict = np.argmax(S)

        if predict != 0:
            pre1L.append(int(row['pre1']))
            pre2L.append(int(row['pre2']))
            pre3L.append(int(row['pre3']))
            curL.append(int(row['cur']))
            nextL.append(int(row['next']))
            predictL.append(int(predict))

            if predict == int(row['next']):
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
    pd.DataFrame(res).to_csv("../res/calc/" + saveName)

    print("Y:" + str(Y))
    print("N:" + str(N))
    print("correct:" + correct)



if __name__ == '__main__':
    spark = SparkSession.builder.appName("calcAccu").master("yarn").enableHiveSupport().getOrCreate()

    get_data_from_local()

    size = range(100000,500000,100000)
    a, b, c = 0.6, 0.3, 0.1
    for i in size:
        precurnextDF = getPrecurnextDF(i)
        DoCalc(precurnextDF.toPandas(), i, a, b, c)

    wx_reminder()

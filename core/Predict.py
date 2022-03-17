import json
import time

import grequests
import numpy as np
import pandas as pd
import requests
from geopy.distance import geodesic
from pyspark.sql import SparkSession
from tqdm import tqdm
from requests.adapters import Retry, HTTPAdapter

from utils.db.df_insert_ignore import save_dataframe
from utils.db.getConn import *
from utils.geo.coordTransform_utils import gcj02_to_wgs84

spark = SparkSession.builder.appName("Predict").master("yarn").enableHiveSupport().getOrCreate()

SOURCE_TABLE = "spark.trackMakeup01"

CROSSING_SINK_TABLE = "crossing"
TRANSFER_SINK_TABLE = "transfer"
ADJACENT_SINK_TABLE = "adjacent"

CROSSING_DISTANCE = 20

clickhouseConn = getClickhouseConn()
mysqlConn = getMysqlConn()
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 591038


def dist(a, b):
    return geodesic(a, b).m


def SinkCrossing():
    df = spark.sql(
        f'SELECT DISTINCT map_longitude,map_latitude from {SOURCE_TABLE} limit 10')

    df = df.toPandas()

    s = requests.session()
    retries = Retry(total=30, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504], raise_on_redirect=True)
    s.mount('https://', HTTPAdapter(max_retries=retries))

    urls = [
        f'https://restapi.amap.com/v3/geocode/regeo?location={r["map_longitude"]},{r["map_latitude"]}&key=30a423f69a6cb59baef9f2f55ce64c41&radius=3000&extensions=all'
        for index, r in df.iterrows()]

    print(urls)
    reqs = [grequests.get(url, session=s) for url in urls]
    map_long_list = [
        float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[0]) for i in
        grequests.map(reqs)]

    map_lat_list = [
        float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[1]) for i in
        grequests.map(reqs)]
    gps_long_list = [
        gcj02_to_wgs84(float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[0]),
                       float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[1]))[0] for i
        in grequests.map(reqs)]

    gps_lat_list = [
        gcj02_to_wgs84(float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[0]),
                       float(str(json.loads(i.text)["regeocode"]["roadinters"][0]["location"]).split(",")[1]))[1] for i
        in grequests.map(reqs)]

    crossing_data = {'map_longitude': map_long_list, 'map_latitude': map_lat_list, 'gps_longitude': gps_long_list,
                     'gps_latitude': gps_lat_list}

    crossing = pd.DataFrame(crossing_data)
    print(crossing)
    save_dataframe(mysqlConn, crossing, CROSSING_SINK_TABLE)

    print("SinkCrossing Done")


def SinkTransfer():
    crossingTempView = "crossingTempView"

    df = pd.read_sql_query(f'select * from {CROSSING_SINK_TABLE}', con=mysqlConn)
    spark.createDataFrame(df).createOrReplaceTempView(crossingTempView)
    spark.sql(f'''
    select   a.car_number as car_number,
             a.gps_time   as gps_time,
             b.id         as id
        from {SOURCE_TABLE} a
               cross join {crossingTempView} b
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


def PrepareData():
    global A, A2, A3
    global Z

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


def DoCalc(precurnextDF):
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

    pd.DataFrame(res).to_csv(
        time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '_' + str(round(Y / (Y + N) * 100, 2)) + '%' + '_' + str(
            CROSSING_DISTANCE) + '.csv')

    print("Y:" + str(Y))
    print("N:" + str(N))
    print("correct:" + str(round(Y / (Y + N) * 100, 2)) + "%")


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


def CalcAccu():
    PrepareData()
    for i in [20000, 40000]:
        precurnextDF = getPrecurnextDF(i)

        DoCalc(precurnextDF.toPandas())


if __name__ == '__main__':
    spark.udf.register("dist", lambda x1, y1, x2, y2: dist((x1, y1), (x2, y2)))

    # SinkCrossing()

    # SinkTransfer()

    # SinkAdjacent()

    CalcAccu()

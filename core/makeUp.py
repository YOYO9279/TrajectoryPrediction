import json
import time
from math import ceil

import grequests
import pandas as pd
import requests
from pandas import DataFrame
from pyspark.sql import SparkSession
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

from utils.geo.coordTransform_utils import gcj02_to_wgs84

SOURCE_TABLE = "spark.track"
SINK_TABLE = "spark.trackMakeup01"

#  需先自创服务 获得sid 填入
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 608418

spark = SparkSession.builder.appName("make up").master("yarn").enableHiveSupport().getOrCreate()
s = requests.session()

retries = Retry(total=30, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504], raise_on_redirect=True)
s.mount('https://', HTTPAdapter(max_retries=retries))


def createTerminal(sourceTableCar):
    urls = [f'https://tsapi.amap.com/v1/track/terminal/add?key={key}&sid={sid}&name={r["car_number"]}' for index, r
            in sourceTableCar.iterrows()]

    reqs = [grequests.get(url, session=s) for url in urls]

    print(reqs)
    print("createTerminal retry")


def uploadTrack(tid, trid, car_num):
    headers = {"Content-Type": "application/json"}
    uploadTrackURL = f"https://tsapi.amap.com/v1/track/point/upload?key={key}"

    df = spark.sql(f'''
        SELECT DISTINCT concat_ws(',', cast(map_longitude as STRING), cast(map_latitude as STRING)) as location,
               gps_time * 1000                                                             as locatetime
        FROM source
        WHERE car_number = '{car_num}'
    ''').toPandas()
    page = ceil(df.shape[0] / 99)
    limit = 99
    offset = 99
    for i in range(page):
        d = {"key": key,
             "sid": sid,
             "tid": tid,
             "trid": trid,
             }
        df99 = df[(int(page) - 1) * int(offset): (int(page) - 1) * int(offset) + int(limit)]
        d["points"] = df99.to_json(orient='records')

        while True:
            try:
                x = requests.post(url=uploadTrackURL, data=json.dumps(d), headers=headers)
                print(x.text)
                break
            except Exception as e:
                time.sleep(1)
                print("uploadTrack retry")


def createuploadtrack(carDF):
    tidList = []
    tridList = []
    car_numberList = []
    for index, row in tqdm(carDF.iterrows(), total=carDF.shape[0], desc="[POST] createuploadTrack"):
        car_number = row["car_number"]
        inquryTrackURL = f'https://tsapi.amap.com/v1/track/terminal/list?key={key}&sid={sid}&name={car_number}'
        while True:
            try:
                tid = requests.get(inquryTrackURL).json()["data"]["results"][0]["tid"]
                break
            except Exception as e:
                time.sleep(1)
                print("inquryTrack retry")
        trid = createTrack(tid)
        uploadTrack(tid, trid, car_number)
        tidList.append(tid)
        tridList.append(trid)
        car_numberList.append(car_number)
    car_info = {"tid": tidList,
                "trid": tridList,
                "car_number": car_numberList}
    car_infoDF = pd.DataFrame(car_info)
    car_infoDF.to_csv('car_info.csv')
    return car_infoDF


def createTrack(tid):
    createTrackURL = f'https://tsapi.amap.com/v1/track/trace/add?key={key}&sid={sid}&tid={tid}'
    resp = s.post(createTrackURL)
    trid = json.loads(resp.text)["data"]["trid"]
    return trid


def getMorePoints(counts, tid, trid):
    page = ceil(counts / 999)

    points = []
    for i in range(page):
        while True:
            try:
                GetMorePointURL = f"https://tsapi.amap.com/v1/track/terminal/trsearch?key={key}&sid={sid}&tid={tid}&trid={trid}&recoup=1&gap=50&pagesize=999&page={i + 1}"
                p = requests.get(GetMorePointURL).json()["data"]["tracks"][0]["points"]
                points += p
                break
            except Exception as e:
                time.sleep(1)
                print("GetMorePoint retry")

    return points


def getPoints(car_infoDF):
    tidL = []
    tridL = []
    car_numL = []
    gps_longitudeL = []
    gps_latitudeL = []
    gps_timeL = []
    map_longitudeL = []
    map_latitudeL = []

    for index, row in tqdm(car_infoDF.iterrows(), total=car_infoDF.shape[0], desc="[GET] MakeUP Points"):
        tid = row["tid"]
        trid = row["trid"]
        car_number = row["car_number"]
        GetPointURL = f"https://tsapi.amap.com/v1/track/terminal/trsearch?key={key}&sid={sid}&tid={tid}&trid={trid}&recoup=1&gap=50&pagesize=999&page=1"
        while True:
            try:
                points = []
                resp = requests.get(GetPointURL).json()
                points += resp["data"]["tracks"][0]["points"]
                counts = resp["data"]["tracks"][0]["counts"]
                if counts > 999:
                    points += getMorePoints(counts, tid, trid)
                j = 0
                for i in points:
                    loc1 = i["location"].split(",")
                    loc2 = gcj02_to_wgs84(float(loc1[0]), float(loc1[1]))
                    map_longitudeL.append(loc1[0])
                    map_latitudeL.append(loc1[1])
                    gps_longitudeL.append(loc2[0])
                    gps_latitudeL.append(loc2[1])
                    tidL.append(tid)
                    tridL.append(trid)
                    car_numL.append(car_number)
                    gps_timeL.append(j)
                    j += 1
                break
            except Exception as e:
                time.sleep(1)
                print("GetPoint Retry")

    res = {"map_longitude": map_longitudeL,
           "map_latitude": map_latitudeL,
           "gps_longitude": gps_longitudeL,
           "gps_latitude": gps_latitudeL,
           "car_number": car_numL,
           "tid": tidL,
           "trid": tridL,
           "gps_time": gps_timeL}
    resDF = DataFrame(res).drop_duplicates(['car_number', 'map_longitude', 'map_latitude'])
    spark.createDataFrame(resDF).write.mode("append").saveAsTable(SINK_TABLE)


if __name__ == '__main__':
    SOURCE_TABLE = spark.sql(f'''
        SELECT map_longitude, map_latitude, gps_time, car_number
        FROM spark.track
        WHERE car_number IN (
            select car_number
            from (SELECT car_number,
                         row_number() OVER ( Order by car_number) AS rl
                  FROM (SELECT car_number
                        FROM (SELECT car_number
                                   , row_number() OVER (PARTITION BY car_number Order by car_number) AS rk
                              FROM spark.track) c
                        WHERE rk = 1) d
                 ) e
                            where rl between 151 and  161
            ORDER BY car_number
        )
        ''').cache().createOrReplaceTempView("source")
    sourceTableCar = spark.sql("SELECT DISTINCT car_number  from source").toPandas()

    createTerminal(sourceTableCar)

    car_infoDF = createuploadtrack(sourceTableCar)

    getPoints(car_infoDF)

    print("done")

import json
import time
from math import ceil

import grequests
import pandas as pd
import requests
from pandas import DataFrame
from pyspark.sql import SparkSession
from requests.adapters import HTTPAdapter, Retry
from tqdm import trange
from utils.geo.coordTransform_utils import gcj02_to_wgs84
import grequests_throttle as gt


def concurQ(reqs):
    resp = gt.map(reqs, rate=50)
    for i in resp:
        try:
            print(i.text)
        except Exception as e:
            print(e)
    return resp


SOURCE_TABLE = "spark.track"
SINK_TABLE = "spark.gretest05"

#  需先自创服务 获得sid 填入
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 608418

spark = SparkSession.builder.appName("make up").master("yarn").enableHiveSupport().getOrCreate()
s = requests.session()


retries = Retry(total=100, backoff_factor=1)
s.mount('https://', HTTPAdapter(max_retries=retries))


def createTerminal(sourceTableCar):
    urls = [f'https://tsapi.amap.com/v1/track/terminal/add?key={key}&sid={sid}&name={r["car_number"]}' for index, r
            in sourceTableCar.iterrows()]

    reqs = [grequests.post(url, session=s) for url in urls]
    concurQ(reqs)

    print("createTerminal Done")


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
    reqs = []
    for i in range(page):
        d = {"key": key,
             "sid": sid,
             "tid": tid,
             "trid": trid,
             }
        df99 = df[(int(page) - 1) * int(offset): (int(page) - 1) * int(offset) + int(limit)]
        d["points"] = df99.to_json(orient='records')

        reqs += [grequests.post(url=uploadTrackURL, session=s, data=json.dumps(d), headers=headers)]
    concurQ(reqs)


def createuploadtrack(carDF):
    urls = [f'https://tsapi.amap.com/v1/track/terminal/list?key={key}&sid={sid}&name={r["car_number"]}' for index, r in
            carDF.iterrows()]
    reqs = [grequests.get(url, session=s) for url in urls]
    car_numberList = [r["car_number"] for index, r in carDF.iterrows()]

    resp = concurQ(reqs)

    tidList = [i.json()["data"]["results"][0]["tid"] for i in resp]
    tridList = [createTrack(i) for i in tidList]
    for i in trange(len(tidList)):
        uploadTrack(tidList[i], tridList[i], car_numberList[i])

    car_info = {"tid": tidList,
                "trid": tridList,
                "car_number": car_numberList}
    car_infoDF = pd.DataFrame(car_info)

    print(car_infoDF)
    car_infoDF.to_csv('car_info.csv')
    return car_infoDF


def createTrack(tid):
    createTrackURL = f'https://tsapi.amap.com/v1/track/trace/add?key={key}&sid={sid}&tid={tid}'
    resp = s.post(createTrackURL)
    trid = json.loads(resp.text)["data"]["trid"]
    return trid


def getMorePoints(counts, tid, trid):
    page = ceil(counts / 999)
    urls = [
        f"https://tsapi.amap.com/v1/track/terminal/trsearch?key={key}&sid={sid}&tid={tid}&trid={trid}&recoup=1&gap=50&pagesize=999&page={i + 2}"
        for i in range(page - 1)]
    reqs = [grequests.get(url, session=s) for url in urls]
    resp = concurQ(reqs)
    points = [j for i in resp for j in i.json()["data"]["tracks"][0]["points"]]

    return points


def sinkPoints(car_infoDF):
    tidL = []
    tridL = []
    car_numL = []
    gps_longitudeL = []
    gps_latitudeL = []
    gps_timeL = []
    map_longitudeL = []
    map_latitudeL = []

    oritidL = [r['tid'] for index, r in car_infoDF.iterrows()]
    oritridL = [r['trid'] for index, r in car_infoDF.iterrows()]
    oricar_numL = [r['car_number'] for index, r in car_infoDF.iterrows()]
    urls = [
        f"https://tsapi.amap.com/v1/track/terminal/trsearch?key={key}&sid={sid}&tid={r['tid']}&trid={r['trid']}&recoup=1&gap=50&pagesize=999&page=1"
        for index, r in car_infoDF.iterrows()]

    reqs = [grequests.get(url, session=s) for url in urls]

    n = 0
    for resp in concurQ(reqs):
        points = []
        points += resp.json()["data"]["tracks"][0]["points"]
        counts = resp.json()["data"]["tracks"][0]["counts"]
        if counts > 999:
            points += getMorePoints(counts, oritidL[n], oritridL[n])
        map_longitudeL += [float(str(i["location"]).split(",")[0]) for i in points]
        map_latitudeL += [float(str(i["location"]).split(",")[1]) for i in points]
        gps_longitudeL += [
            gcj02_to_wgs84(float(str(i["location"]).split(",")[0]), float(str(i["location"]).split(",")[1]))[0] for i in
            points]
        gps_latitudeL += [
            gcj02_to_wgs84(float(str(i["location"]).split(",")[0]), float(str(i["location"]).split(",")[1]))[1] for i in
            points]
        tidL += [oritidL[n] for x in range(len(points))]
        tridL += [oritridL[n] for x in range(len(points))]
        car_numL += [oricar_numL[n] for x in range(len(points))]
        gps_timeL += [x for x in range(len(points))]
        n = n + 1

    res = {"map_longitude": map_longitudeL,
           "map_latitude": map_latitudeL,
           "gps_longitude": gps_longitudeL,
           "gps_latitude": gps_latitudeL,
           "car_number": car_numL,
           "tid": tidL,
           "trid": tridL,
           "gps_time": gps_timeL}
    resDF = DataFrame(res).drop_duplicates(['car_number', 'map_longitude', 'map_latitude'])
    print(resDF)
    spark.createDataFrame(resDF).write.mode("append").saveAsTable(SINK_TABLE)


if __name__ == '__main__':
    SOURCE_TABLE = spark.sql(f'''
        SELECT map_longitude, map_latitude, gps_time, car_number
        FROM {SOURCE_TABLE}
        order by car_number
        limit 100000
        ''')

    SOURCE_TABLE.cache().createOrReplaceTempView("source")
    print(SOURCE_TABLE.count())
    sourceTableCar = spark.sql("SELECT DISTINCT car_number  from source").toPandas()

    print(sourceTableCar)

    createTerminal(sourceTableCar)

    car_infoDF = createuploadtrack(sourceTableCar)

    sinkPoints(car_infoDF)

    print("done")

import json
from math import ceil
import grequests
import pandas as pd
from pyspark.sql import SparkSession
from tqdm import trange
from etc.config import *
from utils.req.concurQ import concurQ


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


def createuploadTrack(carDF):
    urls = [f'https://tsapi.amap.com/v1/track/terminal/list?key={key}&sid={sid}&name={r["car_number"]}' for index, r in
            carDF.iterrows()]
    reqs = [grequests.get(url, session=s) for url in urls]
    car_numberList = [r["car_number"] for index, r in carDF.iterrows()]

    resp = concurQ(reqs)

    tidList = [i.json()["data"]["results"][0]["tid"] for i in resp]
    tridList = createTrack(tidList)
    for i in trange(len(tidList)):
        uploadTrack(tidList[i], tridList[i], car_numberList[i])

    car_info = {"tid": tidList,
                "trid": tridList,
                "car_number": car_numberList}
    car_infoDF = pd.DataFrame(car_info)

    try:
        car_infoDF.to_sql(CARINFO_TABLE, mysqlConn, if_exists="append", index=False)
    except Exception as e:
        print(e)


def createTrack(tidList):
    urls = [f'https://tsapi.amap.com/v1/track/trace/add?key={key}&sid={sid}&tid={tid}' for tid in tidList]
    reqs = [grequests.post(url, session=s) for url in urls]
    resp = concurQ(reqs)
    trid = [json.loads(i.text)["data"]["trid"] for i in resp]
    return trid


if __name__ == '__main__':
    spark = SparkSession.builder.appName("make up").master("yarn").enableHiveSupport().getOrCreate()

    SOURCE_TABLE = spark.sql(f'''
        SELECT map_longitude, map_latitude, gps_time, car_number
        FROM {SOURCE_TABLE}
        WHERE dt = '2018-10-08' AND rk BETWEEN 1 AND 10000
        ''')

    SOURCE_TABLE.cache().createOrReplaceTempView("source")
    print(SOURCE_TABLE.count())
    sourceTableCar = spark.sql("SELECT DISTINCT car_number  from source").toPandas()

    print(sourceTableCar)

    createTerminal(sourceTableCar)

    createuploadTrack(sourceTableCar)

    print("done")

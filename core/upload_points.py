import json
import time
from math import ceil
import grequests
import pandas as pd
from pyspark.sql import SparkSession
from tqdm import trange
from conf.config import *
from utils.db.df_insert_ignore import save_dataframe
from utils.req.concur_request import concurQ
import grequests_throttle as gt
import findspark
findspark.init()

pd.set_option('display.max_rows', None)


def create_terminal(sourceTableCar):
    urls = [f'https://tsapi.amap.com/v1/track/terminal/add?key={key}&sid={sid}&name={r["car_number"]}' for index, r
            in sourceTableCar.iterrows()]

    reqs = [grequests.post(url, session=s) for url in urls]
    resp = gt.map(reqs, rate=30)
    need_delete = [idx for idx, x in enumerate(resp) if x is None or x.json()["errcode"] == 20000]
    sourceTableCar = sourceTableCar.drop(index=need_delete)
    print("clean 20000 ", need_delete)
    print("createTerminal Done")
    return sourceTableCar


def upload_track(tid, trid, car_num):
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

        reqs += [grequests.post(url=uploadTrackURL, session=s, data=json.dumps(d), headers=headers, timeout=5)]
    concurQ(reqs)
    time.sleep(2)


def create_upload_track(carDF):
    urls = [f'https://tsapi.amap.com/v1/track/terminal/list?key={key}&sid={sid}&name={r["car_number"]}' for index, r in
            carDF.iterrows()]
    reqs = [grequests.get(url, session=s) for url in urls]
    car_numberList = [r["car_number"] for index, r in carDF.iterrows()]

    resp = concurQ(reqs)
    tidList = [i.json()["data"]["results"][0]["tid"] for i in resp]
    tridList = create_track(tidList)
    while len(tidList) != len(tridList) != len(car_numberList):
        resp = concurQ(reqs)
        tidList = [i.json()["data"]["results"][0]["tid"] for i in resp]
        tridList = create_track(tidList)

    for i in trange(len(tidList)):
        upload_track(tidList[i], tridList[i], car_numberList[i])

    car_info = {"tid": tidList,
                "trid": tridList,
                "car_number": car_numberList}
    car_infoDF = pd.DataFrame(car_info)

    save_dataframe(mysqlConn, car_infoDF, CARINFO_TABLE)


def create_track(tidList):
    urls = [f'https://tsapi.amap.com/v1/track/trace/add?key={key}&sid={sid}&tid={tid}' for tid in tidList]
    reqs = [grequests.post(url, session=s) for url in urls]
    resp = concurQ(reqs)
    trid = [json.loads(i.text)["data"]["trid"] for i in resp]
    return trid


if __name__ == '__main__':

    step = 100000
    start = 26582601
    stop = 108465083

    for i in range(start, stop, step):
        spark = SparkSession.builder.appName(f"upload Point {i} - {i + step}") \
            .master("yarn").enableHiveSupport().getOrCreate()

        sourceDF = spark.sql(f'''
            SELECT map_longitude, map_latitude, gps_time, translate(car_number,".","0")  as car_number
            FROM {SOURCE_TABLE}
            WHERE dt = '2018-10-08' AND rk BETWEEN {i} AND {i + step}
            ''')

        sourceDF.cache().createOrReplaceTempView("source")
        print(sourceDF.count())
        source_table_car = spark.sql("SELECT DISTINCT car_number  from source").toPandas()


        source_table_car = create_terminal(source_table_car)

        print(source_table_car)

        create_upload_track(source_table_car)

        print(f" {i + step} upload over")

        spark.stop()

        time.sleep(3)

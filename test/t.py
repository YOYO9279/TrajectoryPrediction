import json
import time

import grequests
import requests
from pyspark.sql import SparkSession
from requests.adapters import HTTPAdapter, Retry

key = "30a423f69a6cb59baef9f2f55ce64c41"
SOURCE_TABLE = "spark.track"

spark = SparkSession.builder.appName("Crossing test").master("yarn").enableHiveSupport().getOrCreate()

s = requests.session()

retries = Retry(total=30, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504], raise_on_redirect=True,
                raise_on_status=True)
s.mount('https://', HTTPAdapter(max_retries=retries))




if __name__ == '__main__':
    # spark.udf.register("cross", lambda x, y: queryCrossing(x, y))
    # df = spark.sql(f"select cross(map_longitude,map_latitude) from (SELECT  map_longitude,map_latitude from {SOURCE_TABLE} limit 1000) ").show()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = spark.sql(f"SELECT  map_longitude,map_latitude from {SOURCE_TABLE} limit 1000").toPandas()
    urls = [
        f'https://restapi.amap.com/v3/geocode/regeo?location={r["map_longitude"]},{r["map_latitude"]}&key=30a423f69a6cb59baef9f2f55ce64c41&radius=3000&extensions=all'
        for index, r in df.iterrows()]

    reqs = [grequests.get(url, session=s) for url in urls]
    for i in grequests.map(reqs):
        print(json.loads(i.text)["regeocode"]["roadinters"])

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

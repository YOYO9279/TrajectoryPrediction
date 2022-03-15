import json
import time

import grequests
import requests
from pyspark.sql import SparkSession
from requests.adapters import HTTPAdapter, Retry

# key = "30a423f69a6cb59baef9f2f55ce64c41"
# SOURCE_TABLE = "spark.track"
#
# spark = SparkSession.builder.appName("Crossing test").master("yarn").enableHiveSupport().getOrCreate()
#
# s = requests.session()
#
# retries = Retry(total=30, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504], raise_on_redirect=True,
#                 raise_on_status=True)
# s.mount('https://', HTTPAdapter(max_retries=retries))




if __name__ == '__main__':
    # spark.udf.register("cross", lambda x, y: queryCrossing(x, y))
    # df = spark.sql(f"select cross(map_longitude,map_latitude) from (SELECT  map_longitude,map_latitude from {SOURCE_TABLE} limit 1000) ").show()

    print([(x,y) for x in range(3) for y in range(4)] )

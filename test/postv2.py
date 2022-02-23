import json

import requests
from pyspark.sql import SparkSession

table = 'spark.D49973'

# {
#   "data": {
#     "name": "tt1",
#     "tid": 472968708,
#     "sid": 566697,
#     "trid":20
#   },
#   "errcode": 10000,
#   "errdetail": null,
#   "errmsg": "OK"
# }


# [
# {"location":"116.397428,39.90923",
# "locatetime":1544176895000,
# "speed":40,
# "direction":120,
# "height":39,
# "accuracy":20},
# {"location":"116.397435,39.90935",
# "locatetime":1544176913000,
# "speed":40,
# "direction":110,
# "height":39,
# "accuracy":20}
# ]
if __name__ == '__main__':
    spark = SparkSession.builder.appName("post").master("yarn").enableHiveSupport().getOrCreate()

    df = spark.sql(f'''
        SELECT DISTINCT concat_ws(',', cast(map_longitude as STRING), cast(map_latitude as STRING)) as location,
               gps_time * 1000                                                             as locatetime
        FROM spark.d49973
        limit 99
    ''')

    js = df.toPandas().to_json(orient='records')

    print(js)

    # url = f"https://tsapi.amap.com/v1/track/point/upload?key=30a423f69a6cb59baef9f2f55ce64c41&tid=472968708&sid=566697&trid=40&points={js}"
    url = f"https://tsapi.amap.com/v1/track/point/upload?key=30a423f69a6cb59baef9f2f55ce64c41"

    headers = {"Content-Type": "application/json"}

    d = {}
    d["key"] = "30a423f69a6cb59baef9f2f55ce64c41"
    d["sid"] = 566697
    d["tid"] = 472968708
    d["trid"] = 40
    d["points"] = js
    print(url)
    x = requests.post(url=url, data=json.dumps(d), headers=headers)
    print(x)
    print(x.text)

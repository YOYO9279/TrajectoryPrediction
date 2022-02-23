import json
import math
from collections import defaultdict

import grequests as grequests
import numpy as np
import pandas as pd
import requests
from geopy.distance import geodesic
from pyspark.sql import SparkSession
from requests import Session, Request
from sqlalchemy.orm import declarative_base

from core.getABC import mysql_con, coor_table

# from utils.com.getConfig import getConfig

# conf = getConfig()

if __name__ == '__main__':
    #
    spark = SparkSession.builder.appName("loads coor").master("yarn").enableHiveSupport().getOrCreate()
    # import grequests
    #
    # GDPOSTURL = 'https://restapi.amap.com/v3/geocode/regeo?location={},{}&key=30a423f69a6cb59baef9f2f55ce64c41&radius=3000&extensions=all'
    # #
    # long = ['113.924125','113.918035','113.9231']
    # la = ['22.53724','22.53752','22.53599']
    #
    # req_list = [  # 请求列表
    #     grequests.get(GDPOSTURL.format(long[i],la[i]) for i in range(3) )
    # ]
    #
    # res_list = grequests.map(req_list)  # 并行发送，等最后一个运行完后返回
    # for i in range(3):
    #     print(res_list.text)  # 打印第一个请求的响应文本

    # o = np.linspace(0, 4, 9)
    # o.resize(3, 3)
    # a = o[0] * 0.2 + o[1] * 0.6 + o[2] * 0.2
    # print(a)
    # print(a * a[0])
    # print()
    # d
    # print(np.argmax(o, axis=1)

    # title = 'python'
    # content  = 'content'
    # u =f"http://pushplus.hxtrip.com/send?token=017551c1d95d4f19b1fae1e13efc5e20&title={title}&content={content}&template=json"
    #
    # requests.get(url = u)

    # data = {"key": "30a423f69a6cb59baef9f2f55ce64c41",
    #         "sid": "566697",
    #         "name": "tt2"
    #         }
    #
    # url = "https://tsapi.amap.com/v1/track/terminal/add"
    # response = requests.post(url, data=json.dumps(data))
    # print(response)
    df = pd.read_sql(f'select * from spark.cross100', con=mysql_con)
    df = spark.createDataFrame(df)
    df.write.mode("overwrite").saveAsTable("spark.cross100")
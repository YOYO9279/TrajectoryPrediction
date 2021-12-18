import json
from collections import defaultdict

from geopy.distance import geodesic
from pyspark.sql import SparkSession
from sqlalchemy.orm import declarative_base

from utils.com.getConfig import getConfig

conf = getConfig()

class A:
    a = 11

    def geta(self):
        print(self.a)


if __name__ == '__main__':
    #
    # spark = SparkSession.builder.appName("getABC").master("yarn").enableHiveSupport().getOrCreate()
    #
    # df  = spark.sql("select * from spark.area02 limit 10")
    #
    # df.show()
    #
    # for i ,row in df.toPandas().iterrows():
    #
    #
    # spark.stop()

    # a = np.random.randint(10, 100, size=9)
    # a = a.reshape((3, 3))
    #
    # print(a)
    # print(a[0])
    # print(a[1])
    # print(a[2])
    #
    # print(a.shape)
    # b=np.argmax(a , axis=1)
    # print(b)
    # T = defaultdict(int)
    # T[1] +=1
    # T[2] +=1
    #
    # print(T)
    # a = A()
    # a.geta()

    # print(geodesic((30.28708, 120.12802999999997), (28.7427, 115.86572000000001)).m)
    # df = spark.sql("create table spark.test as SELECT * FROM spark.spark_06051d_coor limit 10")
    #
    # df.show()
    #
    # spark.stop()





    print(conf['table']['hive']['original_table'])
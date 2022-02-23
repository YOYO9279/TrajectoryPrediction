from pyspark.sql import SparkSession
import requests

table = 'spark.D49973'

if __name__ == '__main__':
    spark = SparkSession.builder.appName("post").master("yarn").enableHiveSupport().getOrCreate()

    df = spark.sql(f'''
            SELECT map_longitude                                                                    as x,
                   map_latitude                                                                     as y,
                   direction                                                                        as ag,
                   gps_time - lag(gps_time, 1, 0) OVER (PARTITION BY car_number ORDER BY gps_time ) as tm,
                   gps_speed                                                                        as sp
            
            FROM (
                     SELECT *
                     FROM {table}
                     ORDER BY gps_time
                 ) a
    ''')

    url = 'https://restapi.amap.com/v4/grasproad/driving?key=30a423f69a6cb59baef9f2f55ce64c41'


    js  = df.toPandas().to_json(orient = 'records' )

    print(js)
    x = requests.post(url, data=js)


    print(x.text)





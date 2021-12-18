from pyspark.sql import SparkSession
import pandas as pd
from geopy.distance import geodesic
from utils.db.getConn import getMysqlConn

from utils.com.getConfig import getConfig

config = getConfig()

mysql_conn = getMysqlConn()

## 输入
# 源数据表
original_table = config["table"]["hive"]["original_table"]

# 路口表
coor_table = config["table"]["mysql"]["coor_table"]

# 中间表
coor_car_table = config["table"]["hive"]["coor_car_table"]

## 输出
#
tran_table = config["table"]["mysql"]["tran_table"]


def getCoor():
    df = pd.read_sql_query(f'select * from {coor_table}', con=mysql_conn)
    return spark.createDataFrame(df)


def dist(a, b):
    return geodesic(a, b).m


if __name__ == '__main__':
    spark = SparkSession.builder.appName("getTransfer").master("yarn").enableHiveSupport().getOrCreate()
    df1 = getCoor()
    df1.createOrReplaceTempView(coor_table)
    spark.udf.register("dist", lambda x1, y1, x2, y2: dist((x1, y1), (x2, y2)))

    df2 = spark.sql(
        f"select a.*,b.id from (SELECT map_longitude, map_latitude, gps_time, gps_speed, direction, event, alarm_code, gps_longitude, gps_latitude, altitude, tachometer_speed, miles, error_type, access_code, receiving_time, color, province, car_number, dt from (SELECT *,row_number() over(PARTITION BY gps_longitude, gps_latitude ORDER BY province) rank from {original_table}  ) m WHERE m.rank=1) a cross join {coor_table} b on dist(a.gps_latitude,a.gps_longitude,b.gps_latitude,b.gps_longitude) < 10")

    df2.show()

    df2.createTempView(coor_car_table)

    # df2.write.format("hive").mode("overwrite").saveAsTable('spark.' + coor_car_table)

    # print("sink to hive Done")



    df3 = spark.sql(
        f"select c_from_id as from_id, c_to_id as to_id, count(1) as cnt from(select a.id as c_from_id, b.id as c_to_id from {coor_car_table} a cross join {coor_car_table} b where a.gps_time < b.gps_time and a.car_number = b.car_number) c group by from_id, to_id")

    ## TODO 这里是repalce
    df3.toPandas().to_sql(tran_table, con=mysql_conn, index=False, if_exists='replace')

    spark.stop()

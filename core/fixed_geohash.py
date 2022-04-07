from pyspark.sql import SparkSession
import geohash2

if __name__ == '__main__':
    spark = SparkSession.builder.appName("add_geohash").master("yarn").enableHiveSupport().getOrCreate()
    spark.udf.register("geohash", lambda x, y: geohash2.encode(x, y))
    df = spark.sql(
        f"INSERT OVERWRITE TABLE spark.ods_track_rk_geohash  SELECT *,geohash(map_latitude,map_longitude) as geohash from spark.ods_track_rk WHERE dt = '2018-10-08'")
    spark.stop()

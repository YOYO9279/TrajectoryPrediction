import requests
from requests.adapters import HTTPAdapter

from utils.db.get_conn import getClickhouseConn, getMysqlConn

UPLOAD_SOURCE_TABLE = "spark.ods_track_rk"

CALC_SOURCE_TABLE = "spark.ods_track_rk_geohash"

# mysql
CROSSING_SINK_TABLE = "crossing_geohash"

# clickhouse
TRANSFER_SINK_TABLE = "transfer_geohash_makeup"
ADJACENT_SINK_TABLE = "new_adj"
CARINFO_TABLE = "carinfo02"

CROSSING_DISTANCE = 20

suffix = "_makeup01"

A_PATH = "../data/A" + suffix
A2_PATH = "../data/A2" + suffix
A3_PATH = "../data/A3" + suffix
Z_PATH = "../data/Z" + suffix

s = requests.session()
adapter = requests.adapters.HTTPAdapter(pool_connections=200,
                                        pool_maxsize=200)
s.mount('https://', adapter)

clickhouseConn = getClickhouseConn()
mysqlConn = getMysqlConn()
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 591038

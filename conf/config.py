import requests
from requests.adapters import HTTPAdapter

from utils.db.get_conn import getClickhouseConn, getMysqlConn

SOURCE_TABLE = "spark.ods_track_rk"

# mysql
CROSSING_SINK_TABLE = "crossing_geohash"

# clickhouse
TRANSFER_SINK_TABLE = "transfer"
ADJACENT_SINK_TABLE = "adjacent"
CARINFO_TABLE = "carinfo"

CROSSING_DISTANCE = 20

s = requests.session()
adapter = requests.adapters.HTTPAdapter(pool_connections=200,
                                        pool_maxsize=200)
s.mount('https://', adapter)

clickhouseConn = getClickhouseConn()
mysqlConn = getMysqlConn()
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 591038

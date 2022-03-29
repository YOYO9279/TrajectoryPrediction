import requests
from requests.adapters import HTTPAdapter, Retry

from utils.db.get_conn import getClickhouseConn, getMysqlConn

SOURCE_TABLE = "spark.ods_track_rk"

#mysql
CROSSING_SINK_TABLE = "crossing"

#clickhouse
TRANSFER_SINK_TABLE = "transfer"
ADJACENT_SINK_TABLE = "adjacent"
CARINFO_TABLE = "carinfo"

CROSSING_DISTANCE = 20

s = requests.session()
retries = Retry(total=10, backoff_factor=0.1)
s.mount('https://', HTTPAdapter(max_retries=retries))



clickhouseConn = getClickhouseConn()
mysqlConn = getMysqlConn()
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 591038

from utils.db.getConn import getClickhouseConn, getMysqlConn

SOURCE_TABLE = "spark.trackMakeup01"

CROSSING_SINK_TABLE = "crossing"
TRANSFER_SINK_TABLE = "transfer"
ADJACENT_SINK_TABLE = "adjacent"

CROSSING_DISTANCE = 20

clickhouseConn = getClickhouseConn()
mysqlConn = getMysqlConn()
key = "30a423f69a6cb59baef9f2f55ce64c41"
sid = 591038

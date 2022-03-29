from sqlalchemy import create_engine




def getMysqlConn():
    return create_engine(r"mysql+mysqldb://root:123456@node01/spark?charset=utf8")

def getClickhouseConn():
    return create_engine(r"clickhouse://default:@node01/ck_spark")

from sqlalchemy import create_engine




def getMysqlConn():
    return create_engine(r"mysql+mysqldb://root:123456@node01/spark")

def getClickhouseConn():
    return create_engine(r"clickhouse://default:@node01/ck_spark")

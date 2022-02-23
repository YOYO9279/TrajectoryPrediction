import random
import string
import pandas as pd


def _table_column_names(conn, table: str) -> str:
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
    rows = conn.execute(query)
    dirty_names = [i[0] for i in rows]
    clean_names = '`' + '`, `'.join(map(str, dirty_names)) + '`'
    return clean_names


def _insert_conflict_ignore(conn, df: pd.DataFrame, table: str):
    conn.execute(f"alter table {table} AUTO_INCREMENT=1")
    temp_table = ''.join(random.choice(string.ascii_letters) for i in range(10))
    try:
        df.to_sql(temp_table, conn, if_exists='replace', index=False)
        columns = _table_column_names(conn=conn, table=temp_table)
        insert_query = f'INSERT IGNORE INTO {table}({columns}) SELECT {columns} FROM `{temp_table}`'
        conn.execute(insert_query)
    except Exception as e:
        print(e)
    finally:
        drop_query = f'DROP TABLE IF EXISTS `{temp_table}`'
        conn.execute(drop_query)


def save_dataframe(conn, df: pd.DataFrame, table: str):
    if df.index.name is None:
        save_index = False
    else:
        save_index = True

    _insert_conflict_ignore(conn=conn, df=df, table=table)

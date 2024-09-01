
import pandas as pd
from pipeline_import.configs import postgres_cfg


def run_remote_sql_query(sql, **params) -> pd.DataFrame:
    pg_cfg = postgres_cfg()

    db_conn_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'
    db_conn_string = db_conn_string.format(pg_cfg.user,
                                           pg_cfg.password,
                                           pg_cfg.host,
                                           pg_cfg.port,
                                           pg_cfg.database)

    df: pd.DataFrame = pd.read_sql_query(sql, db_conn_string, params=params)

    return df


def query_for_column(table, column):
    sql = f"""SELECT DISTINCT {column} FROM {table};"""
    df = run_remote_sql_query(sql)
    return df[column]

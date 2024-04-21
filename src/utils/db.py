
import pandas as pd
import psycopg2
from pipeline_import.configs import postgres_cfg


def run_remote_sql_query(sql, **params) -> pd.DataFrame:
    pg_cfg = postgres_cfg()
    user = pg_cfg.user
    password = pg_cfg.password
    host = pg_cfg.host
    port = pg_cfg.port
    database = pg_cfg.database

    db = psycopg2.connect(host=host,
                          database=database,
                          user=user,
                          password=password,
                          port=port,
                          )

    df: pd.DataFrame = pd.read_sql_query(sql, db, params=params)

    return df


def query_for_column(table, column):
    sql = f"""SELECT DISTINCT {column} FROM {table};"""
    df = run_remote_sql_query(sql)
    return df[column]

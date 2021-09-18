#! /usr/bin/env python

import psycopg2
import pandas as pd
from getpass import getpass

pd.options.display.max_columns = 999

user = getpass()
password = getpass()
ip = getpass()
port = getpass()
dbname = getpass()

db_connection_string = 'postgresql://{}:{}@{}:{}/{}'

with psycopg2.connect(db_connection_string.format(user,
                                                  password,
                                                  ip,
                                                  port,
                                                  dbname)) as con:

    sql = """SELECT game_evals.game_link,
                    chess_games.player_elo,
                    half_move,
                    result,
                    evaluation
             from game_evals
             JOIN chess_games on chess_games.game_link = game_evals.game_link
             WHERE evaluation not in (-9999, 9999)
             --and chess_games.player_elo > 2200
             and chess_games.time_control_category = 'blitz'
             and random() < 0.045
             --tablesample can't be used with non-materialized views
             ;
             """
    df = pd.read_sql_query(sql, con)

print(f'Raw dataset shape: {df.shape}')
print('Example rows from dataset:')
print(df.head())

df.to_csv('../../../data/raw/dataset.csv', index=False)

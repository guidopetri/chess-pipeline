#! /usr/bin/env python

import os

import pandas as pd
import psycopg2
from dotenv import find_dotenv, load_dotenv

pd.options.display.max_columns = 999

load_dotenv(find_dotenv())

user = os.environ.get('CHESS_PIPELINE_USER')
password = os.environ.get('CHESS_PIPELINE_PASSWORD')
ip = os.environ.get('CHESS_PIPELINE_HOST')
port = os.environ.get('CHESS_PIPELINE_PORT')
dbname = os.environ.get('CHESS_PIPELINE_DB')

db_connection_string = 'postgresql://{}:{}@{}:{}/{}'

with psycopg2.connect(db_connection_string.format(user,
                                                  password,
                                                  ip,
                                                  port,
                                                  dbname)) as con:

    sql = """SELECT games.game_link,
                    games.player_color,
                    games.player_elo,
                    games.opponent_elo,
                    games.player_elo - games.opponent_elo as elo_diff,
                    clocks.half_move,
                    fen,
                    increment,
                    (increment > 0)::int as has_increment,
                    result,
                    evaluation,
                    clock
             from game_clocks clocks
             -- tablesample bernoulli(1.5) repeatable(13)
             JOIN chess_games games on games.game_link = clocks.game_link
             LEFT JOIN game_evals evals on evals.game_link = clocks.game_link
                  and evals.half_move = clocks.half_move
             WHERE
                 -- evaluation not in (-9999, 9999) and
                 time_control_category in ('blitz')
             and games.player_elo > 2800
             --ORDER BY games.game_link asc, clocks.half_move asc
             ;
             """
    df = pd.read_sql_query(sql, con)

print(f'Raw dataset shape: {df.shape}')
print('Example rows from dataset:')
print(df.head())

df.to_csv('../../data/raw/dataset.csv', index=False)

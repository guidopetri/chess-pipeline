#! /usr/bin/env python3

import re
from pathlib import Path
from subprocess import SubprocessError
from typing import Type

import lichess.api
import pandas as pd
import stockfish
from chess.pgn import Game
from pandas import (
    Series,
    concat,
    merge,
    read_sql_query,
    to_datetime,
    to_numeric,
    to_timedelta,
)
from psycopg2 import connect
from utils.types import Json, Visitor


def get_sf_evaluation(fen: str,
                      sf_location: Path,
                      sf_depth: int,
                      ) -> float | None:
    # get cloud eval if available
    try:
        cloud_eval = lichess.api.cloud_eval(fen=fen, multiPv=1)
        rating = cloud_eval['pvs'][0]
        if 'cp' in rating:
            rating = rating['cp'] / 100
        elif 'mate' in rating:
            rating = -9999 if rating['mate'] < 0 else 9999
        else:
            raise KeyError(f'{fen}, {rating}')
        return rating
    except lichess.api.ApiHttpError:
        # continue execution
        pass

    # implicit else
    sf = stockfish.Stockfish(sf_location,
                             depth=sf_depth)

    sf.set_fen_position(fen)
    if sf.get_best_move() is not None:
        rating_match = re.search(r'score (cp|mate) (.+?)(?: |$)',
                                 sf.info)
        if rating_match is None:
            raise SubprocessError('Could not find chess engine rating'
                                  f' in info string: {sf.info}')

        if rating_match.group(1) == 'mate':
            original_rating = int(rating_match.group(2))

            # adjust ratings for checkmate sequences
            if original_rating:
                rating = 999900 * original_rating / abs(original_rating)
            elif ' w ' in fen:
                # TODO: is this + the else clause necessary? the UCI syntax
                # returns `mate X` which seems to always be a number
                rating = 999900
            else:
                rating = -999900
        else:
            rating = int(rating_match.group(2))
        if ' b ' in fen:
            rating *= -1
        rating /= 100
    else:
        rating = None

    return rating


def parse_headers(game: Game, visitors: list[Type[Visitor]]) -> Json:
    if getattr(game.headers, 'Variant', '') in ['From Position', '']:
        game.headers['Variant'] = 'Standard'
    for visitor in visitors:
        game.accept(visitor(game))
    game_infos: Json = Json({x: y for x, y in game.headers.items()})
    game_infos['moves'] = [x.san() for x in game.mainline()]

    return game_infos


def fix_provisional_columns(json_df: pd.DataFrame) -> pd.DataFrame:
    for side in ['black', 'white']:
        col = f'players_{side}_provisional'
        if col in json_df.columns:
            json_df[col].fillna(False, inplace=True)
        else:
            json_df[col] = False
    return json_df


def convert_clock_to_seconds(clocks):
    clocks = to_timedelta(clocks,
                          errors='coerce')
    clocks = clocks.dt.total_seconds().copy()
    clocks.fillna(-1.0, inplace=True)
    clocks = clocks.astype(int)

    return clocks


def get_clean_fens(positions: pd.Series) -> pd.Series:
    # split, get all but last element of resulting list, then re-join
    return positions.str.split().str[:-1].str.join(' ')


def transform_game_data(df, player):
    df['player'] = player

    if 'black_rating_diff' not in df.columns:
        df['black_rating_diff'] = 0

    if 'white_rating_diff' not in df.columns:
        df['white_rating_diff'] = 0

    # add two strings and remove the player name so that we don't
    # have to use pd.DataFrame.apply
    df['opponent'] = df['white'] + df['black']
    df['opponent'] = df['opponent'].str.replace(player, '')

    series_player_black = df['black'] == player
    df['player_color'] = series_player_black.map({True: 'black',
                                                  False: 'white',
                                                  })
    df['opponent_color'] = series_player_black.map({False: 'black',
                                                    True: 'white',
                                                    })

    df['player_elo'] = ((series_player_black
                         * df['black_elo'])
                        + (~series_player_black
                            * df['white_elo']))
    df['opponent_elo'] = ((series_player_black
                           * df['white_elo'])
                          + (~series_player_black
                              * df['black_elo']))

    df['player_rating_diff'] = ((series_player_black
                                 * df['black_rating_diff'])
                                + (~series_player_black
                                    * df['white_rating_diff']))

    df['opponent_rating_diff'] = ((series_player_black
                                   * df['white_rating_diff'])
                                  + (~series_player_black
                                      * df['black_rating_diff']))

    # another helper series
    series_result = df['result'] + series_player_black.astype(str)
    df['player_result'] = series_result.map({'0-1True': 'Win',
                                             '1-0False': 'Win',
                                             '1/2-1/2True': 'Draw',
                                             '1/2-1/2False': 'Draw',
                                             '1-0True': 'Loss',
                                             '0-1False': 'Loss',
                                             })

    df['opponent_result'] = series_result.map({'0-1True': 'Loss',
                                               '1-0False': 'Loss',
                                               '1/2-1/2True': 'Draw',
                                               '1/2-1/2False': 'Draw',
                                               '1-0True': 'Win',
                                               '0-1False': 'Win',
                                               })

    df.rename(columns={'speed': 'time_control_category'},
              inplace=True)

    df['datetime_played'] = to_datetime(df['utc_date_played'].astype(str)
                                        + ' '
                                        + df['time_played'].astype(str))
    df['starting_time'] = df['time_control'].str.extract(r'(\d+)\+')
    df['increment'] = df['time_control'].str.extract(r'\+(\d+)').astype(int)

    df['in_arena'] = df['event_type'].str.contains(r'Arena')
    df['in_arena'] = df['in_arena'].map({True: 'In arena',
                                         False: 'Not in arena'})

    df['rated_casual'] = df['event_type'].str.contains('Casual')
    df['rated_casual'] = df['rated_casual'].map({True: 'Casual',
                                                 False: 'Rated'})

    mapping_dict = {True: 'Queen exchange',
                    False: 'No queen exchange',
                    }
    df['queen_exchange'] = df['queen_exchange'].map(mapping_dict)

    # figure out castling sides
    castling_df = df[['game_link',
                      'player_color',
                      'opponent_color',
                      'castling_sides']]
    # i thought the following would be easier with pandas 0.25.0's
    # pd.DataFrame.explode() but because we use dicts, it isn't

    # convert dict to dataframe cells
    castling_df = concat([castling_df.drop('castling_sides', axis=1),
                          castling_df['castling_sides'].apply(Series)],
                         axis=1)
    castling_df.fillna('No castling', inplace=True)
    castle_helper_srs = castling_df['player_color'] == 'black'
    castling_df['player_castling_side'] = ((~castle_helper_srs)
                                           * castling_df['white']
                                           + castle_helper_srs
                                           * castling_df['black'])
    castling_df['opponent_castling_side'] = ((~castle_helper_srs)
                                             * castling_df['black']
                                             + castle_helper_srs
                                             * castling_df['white'])

    castling_df = castling_df[['game_link',
                               'player_castling_side',
                               'opponent_castling_side',
                               ]]

    df = merge(df,
               castling_df,
               on='game_link')

    # type handling
    df['date_played'] = to_datetime(df['date_played'])
    df['utc_date_played'] = to_datetime(df['utc_date_played'])

    rating_columns = ['player_elo',
                      'player_rating_diff',
                      'opponent_elo',
                      'opponent_rating_diff'
                      ]

    for column in rating_columns:
        # ? ratings are anonymous players
        df[column] = df[column].replace('?', '1500')
        df[column] = to_numeric(df[column])

    return df


def get_color_stats(df):
    color_stats = df.groupby(['time_control_category',
                              'player_color',
                              'player_result']).agg({'id': 'nunique'})
    color_stats.reset_index(drop=False, inplace=True)

    # pivot so the columns are the player result
    color_stats = color_stats.pivot_table(index=['time_control_category',
                                                 'player_color'],
                                          columns='player_result',
                                          values='id')

    # divide each row by the sum of the row
    color_stats = color_stats.div(color_stats.sum(axis=1), axis=0)
    color_stats.fillna(0, inplace=True)

    # reorder columns
    for col in ['Win', 'Draw', 'Loss']:
        if col not in color_stats:
            color_stats[col] = 0
    color_stats = color_stats[['Win', 'Draw', 'Loss']]
    return color_stats


def get_elo_by_weekday(df, category='blitz'):
    df = df[df['time_control_category'] == category]
    df['weekday_played'] = df['datetime_played'].dt.weekday

    # change to sunday-first, not monday-first
    df['weekday_played'].replace(6, -1, inplace=True)
    df['weekday_played'] += 1  # what a dumb way of fixing this

    elo = (df.groupby('weekday_played')
             .agg({'player_elo': ['mean',
                                  'std',
                                  'min',
                                  'max']}))
    elo.fillna(0, inplace=True)
    # drop the first index on columns
    elo = (elo.T
              .reset_index(level=0, drop=True)
              .T
              .reset_index(drop=False))
    elo.sort_values(by='weekday_played', inplace=True)
    return elo


def get_weekly_data(pg_cfg, player):
    db_connection_string = 'postgresql://{}:{}@{}:{}/{}'

    with connect(db_connection_string.format(pg_cfg.read_user,
                                             pg_cfg.read_password,
                                             pg_cfg.host,
                                             pg_cfg.port,
                                             pg_cfg.database)) as con:

        sql = f"""SELECT * from chess_games
                  WHERE player = '{player}'
                  AND datetime_played >= now()::date - interval '7 days';
               """

        df = read_sql_query(sql, con)
    return df

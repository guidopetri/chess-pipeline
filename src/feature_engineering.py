import pandas as pd
from pipeline_import.transforms import (
    convert_clock_to_seconds,
    fix_provisional_columns,
    get_clean_fens,
)


def clean_chess_df(pgn: pd.DataFrame, json: pd.DataFrame) -> pd.DataFrame:
    json['Site'] = 'https://lichess.org/' + json['id']

    json = fix_provisional_columns(json)

    json = json[['Site',
                 'speed',
                 'status',
                 'players_black_provisional',
                 'players_white_provisional',
                 ]]

    df: pd.DataFrame = pd.merge(pgn, json, on='Site')

    # rename columns
    df.rename(columns={'Black':                     'black',
                       'BlackElo':                  'black_elo',
                       'BlackRatingDiff':           'black_rating_diff',
                       'Date':                      'date_played',
                       'ECO':                       'opening_played',
                       'Event':                     'event_type',
                       'Result':                    'result',
                       'Round':                     'round',
                       'Site':                      'game_link',
                       'Termination':               'termination',
                       'TimeControl':               'time_control',
                       'UTCDate':                   'utc_date_played',
                       'UTCTime':                   'time_played',
                       'Variant':                   'chess_variant',
                       'White':                     'white',
                       'WhiteElo':                  'white_elo',
                       'WhiteRatingDiff':           'white_rating_diff',
                       'Opening':                   'lichess_opening',
                       'players_black_provisional': 'black_elo_tentative',
                       'players_white_provisional': 'white_elo_tentative',
                       },
              inplace=True)
    return df


def explode_moves(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['game_link', 'moves']]

    df = df.explode('moves')
    df.rename(columns={'moves': 'move'},
              inplace=True)
    df['half_move'] = df.groupby('game_link').cumcount() + 1
    return df


def explode_clocks(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['game_link', 'clocks']]

    df = df.explode('clocks')
    df.rename(columns={'clocks': 'clock'},
              inplace=True)
    df['half_move'] = df.groupby('game_link').cumcount() + 1
    df['clock'] = convert_clock_to_seconds(df['clock'])
    return df


def explode_positions(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['game_link', 'positions']]

    df = df.explode('positions')
    df.rename(columns={'positions': 'position'},
              inplace=True)
    df['half_move'] = df.groupby('game_link').cumcount() + 1

    df['fen'] = get_clean_fens(df['position'])
    return df


def explode_materials(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['game_link', 'material_by_move']]

    df = df.explode('material_by_move')

    df = pd.concat([df['game_link'],
                    df['material_by_move'].apply(pd.Series)
                                          .fillna(0)
                                          .astype(int)],
                   axis=1)
    df.rename(columns={'r': 'rooks_black',
                       'n': 'knights_black',
                       'b': 'bishops_black',
                       'q': 'queens_black',
                       'p': 'pawns_black',
                       'P': 'pawns_white',
                       'R': 'rooks_white',
                       'N': 'knights_white',
                       'B': 'bishops_white',
                       'Q': 'queens_white',
                       },
              inplace=True)

    df['half_move'] = df.groupby('game_link').cumcount() + 1
    return df

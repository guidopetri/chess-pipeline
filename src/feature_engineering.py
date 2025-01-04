from datetime import date
from pathlib import Path

import pandas as pd
from pipeline_import.transforms import (
    convert_clock_to_seconds,
    fix_provisional_columns,
    get_clean_fens,
)
from utils.output import get_output_file_prefix


def clean_chess_df(player: str,
                   perf_type: str,
                   data_date: date,
                   local_stockfish: bool,
                   io_dir: Path,
                   ) -> None:
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    json = pd.read_parquet(io_dir / f'{prefix}_raw_json.parquet')
    pgn = pd.read_parquet(io_dir / f'{prefix}_raw_pgn.parquet')

    if pgn.empty and json.empty:
        pgn.to_parquet(io_dir / f'{prefix}_cleaned_df.parquet')
    elif pgn.empty or json.empty:
        raise ValueError('Found only one of pgn/json empty for input '
                         f'{player=} {perf_type=} {data_date=} '
                         f'{io_dir=}')

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
    df.to_parquet(io_dir / f'{prefix}_cleaned_df.parquet')


def explode_moves(player: str,
                  perf_type: str,
                  data_date: date,
                  local_stockfish: bool,
                  io_dir: Path,
                  ) -> None:
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    df = pd.read_parquet(io_dir / f'{prefix}_cleaned_df.parquet')
    df = df[['game_link', 'moves']]

    df = df.explode('moves')
    df.rename(columns={'moves': 'move'},
              inplace=True)
    df['half_move'] = df.groupby('game_link').cumcount() + 1
    df.to_parquet(io_dir / f'{prefix}_exploded_moves.parquet')


def explode_clocks(player: str,
                   perf_type: str,
                   data_date: date,
                   local_stockfish: bool,
                   io_dir: Path,
                   ) -> None:
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    df = pd.read_parquet(io_dir / f'{prefix}_cleaned_df.parquet')
    df = df[['game_link', 'clocks']]

    df = df.explode('clocks')
    df.rename(columns={'clocks': 'clock'},
              inplace=True)
    df['half_move'] = df.groupby('game_link').cumcount() + 1
    df['clock'] = convert_clock_to_seconds(df['clock'])
    df.to_parquet(io_dir / f'{prefix}_exploded_clocks.parquet')


def explode_positions(player: str,
                      perf_type: str,
                      data_date: date,
                      local_stockfish: bool,
                      io_dir: Path,
                      ) -> None:
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    df = pd.read_parquet(io_dir / f'{prefix}_cleaned_df.parquet')
    df = df[['game_link', 'positions']]

    df = df.explode('positions')
    df.rename(columns={'positions': 'position'},
              inplace=True)
    df['half_move'] = df.groupby('game_link').cumcount() + 1

    df['fen'] = get_clean_fens(df['position'])
    df.to_parquet(io_dir / f'{prefix}_exploded_positions.parquet')


def explode_materials(player: str,
                      perf_type: str,
                      data_date: date,
                      local_stockfish: bool,
                      io_dir: Path,
                      ) -> None:
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    df = pd.read_parquet(io_dir / f'{prefix}_cleaned_df.parquet')
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
    df.to_parquet(io_dir / f'{prefix}_exploded_materials.parquet')

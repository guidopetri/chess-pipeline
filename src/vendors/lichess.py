from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Type

import lichess.api
import pandas as pd
from chess.pgn import Game
from lichess.format import JSON, PYCHESS
from pipeline_import.configs import get_cfg
from pipeline_import.transforms import parse_headers
from pipeline_import.visitors import (
    CastlingVisitor,
    ClocksVisitor,
    EvalsVisitor,
    MaterialVisitor,
    PositionsVisitor,
    PromotionsVisitor,
    QueenExchangeVisitor,
)
from utils.output import get_output_file_prefix
from utils.types import Json, Visitor
from zoneinfo import ZoneInfo


def fetch_lichess_api_json(player: str,
                           perf_type: str,
                           data_date: date,
                           local_stockfish: bool,
                           io_dir: Path,
                           ) -> None:
    data_datetime = datetime(data_date.year,
                             data_date.month,
                             data_date.day,
                             tzinfo=ZoneInfo('GMT'),
                             )
    next_datetime: datetime = data_datetime + timedelta(days=1)
    until_unix: int = 1000 * int(next_datetime.timestamp())
    since_unix: int = 1000 * int(data_datetime.timestamp())

    token = get_cfg('lichess')['token']

    games: list[Json] = lichess.api.user_games(player,
                                               since=since_unix,
                                               until=until_unix,
                                               perfType=perf_type,
                                               auth=token,
                                               evals='false',
                                               clocks='false',
                                               moves='false',
                                               format=JSON,
                                               )

    df: pd.DataFrame = pd.json_normalize([game for game in games], sep='_')
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    df.to_parquet(io_dir / f'{prefix}_raw_json.parquet')


def fetch_lichess_api_pgn(player: str,
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
    game_count = len(json)

    data_datetime = datetime(data_date.year,
                             data_date.month,
                             data_date.day,
                             tzinfo=ZoneInfo('GMT'),
                             )
    next_datetime: datetime = data_datetime + timedelta(days=1)
    until_unix: int = 1000 * int(next_datetime.timestamp())
    since_unix: int = 1000 * int(data_datetime.timestamp())

    token = get_cfg('lichess')['token']

    games: list[Game] = lichess.api.user_games(player,
                                               since=since_unix,
                                               until=until_unix,
                                               perfType=perf_type,
                                               auth=token,
                                               clocks='true',
                                               evals='true',
                                               opening='true',
                                               format=PYCHESS,
                                               )

    visitors: list[Type[Visitor]] = [EvalsVisitor,
                                     ClocksVisitor,
                                     QueenExchangeVisitor,
                                     CastlingVisitor,
                                     PromotionsVisitor,
                                     PositionsVisitor,
                                     MaterialVisitor,
                                     ]

    header_infos = []

    counter: int = 0

    for game in games:
        game_infos: Json = parse_headers(game, visitors)
        header_infos.append(game_infos)

        # progress bar stuff
        counter += 1

        current: str = f'{game_infos["UTCDate"]} {game_infos["UTCTime"]}'

        current_progress: float = counter / game_count
        print(f'Parsed until {current} :: '
              f'{counter} / {game_count} :: {current_progress:.2%}')

    df: pd.DataFrame = pd.DataFrame(header_infos)
    df.to_parquet(io_dir / f'{prefix}_raw_pgn.parquet')

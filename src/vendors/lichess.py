from calendar import timegm
from datetime import datetime, timedelta
from typing import Type

import lichess.api
import pandas as pd
from chess.pgn import Game
from lichess.format import JSON, PYCHESS
from luigi import Task
from pipeline_import.configs import lichess_token
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
from utils.types import Json, Visitor


def fetch_lichess_api_json(player: str,
                           perf_type: str,
                           since: datetime,
                           single_day: bool,
                           ) -> pd.DataFrame:
    if single_day:
        unix_time_until: int = timegm((since + timedelta(days=1)).timetuple())
    else:
        unix_time_until = timegm(datetime.today().date().timetuple())
    until: int = int(1000 * unix_time_until)

    unix_time_since: int = timegm(since.timetuple())
    since_unix: int = int(1000 * unix_time_since)

    games: list[Json] = lichess.api.user_games(player,
                                               since=since_unix,
                                               until=until,
                                               perfType=perf_type,
                                               auth=lichess_token().token,
                                               evals='false',
                                               clocks='false',
                                               moves='false',
                                               format=JSON)

    df: pd.DataFrame = pd.json_normalize([game for game in games], sep='_')
    return df


def fetch_lichess_api_pgn(player: str,
                          perf_type: str,
                          since: datetime,
                          single_day: bool,
                          game_count: int,
                          task: Task,
                          ) -> pd.DataFrame:
    if single_day:
        unix_time_until: int = timegm((since + timedelta(days=1)).timetuple())
    else:
        unix_time_until = timegm(datetime.today().date().timetuple())
    until: int = int(1000 * unix_time_until)

    unix_time_since: int = timegm(since.timetuple())
    since_unix: int = int(1000 * unix_time_since)

    games: list[Game] = lichess.api.user_games(player,
                                               since=since_unix,
                                               until=until,
                                               perfType=perf_type,
                                               auth=lichess_token().token,
                                               clocks='true',
                                               evals='true',
                                               opening='true',
                                               format=PYCHESS)

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
        task.set_status_message(f'Parsed until {current} :: '
                                f'{counter} / {game_count}')
        task.set_progress_percentage(round(current_progress * 100, 2))

    df: pd.DataFrame = pd.DataFrame(header_infos)

    task.set_status_message('Parsed all games')
    task.set_progress_percentage(100)
    return df

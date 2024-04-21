#! /usr/bin/env python3

import hashlib
import os
from calendar import timegm
from datetime import datetime, timedelta
from typing import Type

import lichess.api
import pandas as pd
import psycopg2
from chess.pgn import Game
from lichess.format import JSON, PYCHESS
from luigi import LocalTarget, Task
from luigi.format import Nop
from luigi.parameter import BoolParameter, DateParameter, Parameter
from luigi.util import inherits, requires
from pipeline_import.configs import lichess_token, postgres_cfg, stockfish_cfg
from pipeline_import.models import predict_wp
from pipeline_import.postgres_templates import (
    CopyWrapper,
    HashableDict,
    TransactionFactTable,
)
from pipeline_import.transforms import (
    convert_clock_to_seconds,
    fix_provisional_columns,
    get_clean_fens,
    get_sf_evaluation,
    parse_headers,
    transform_game_data,
)
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


def run_remote_sql_query(sql, **params):
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

    df = pd.read_sql_query(sql, db, params=params)

    return df


def query_for_column(table, column):
    sql = f"""SELECT DISTINCT {column} FROM {table};"""
    df = run_remote_sql_query(sql)
    return df[column]


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


class FetchLichessApiJSON(Task):

    player = Parameter(default='thibault')
    perf_type = Parameter(default='blitz')
    since = DateParameter(default=datetime.today().date() - timedelta(days=1))
    single_day = BoolParameter()

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-raw-games-'
                         f'{self.player}-{self.perf_type}-json.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()
        df = fetch_lichess_api_json(self.player,
                                    self.perf_type,
                                    self.since,
                                    self.single_day,
                                    )
        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(FetchLichessApiJSON)
class FetchLichessApiPGN(Task):

    player = Parameter(default='thibault')
    perf_type = Parameter(default='blitz')
    since = DateParameter(default=datetime.today().date() - timedelta(days=1))
    single_day = BoolParameter()

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-raw-games-'
                         f'{self.player}-{self.perf_type}-pgn.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            json = pd.read_pickle(f, compression=None)
            game_count = len(json)

        df = fetch_lichess_api_pgn(self.player,
                                   self.perf_type,
                                   self.since,
                                   self.single_day,
                                   game_count,
                                   self,
                                   )

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(FetchLichessApiPGN, FetchLichessApiJSON)
class CleanChessDF(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-cleaned-games-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input()[0].open('r') as f:
            pgn = pd.read_pickle(f, compression=None)

        with self.input()[1].open('r') as f:
            json = pd.read_pickle(f, compression=None)

        if pgn.empty and json.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                pgn.to_pickle(temp_output_path, compression=None)

            return
        elif pgn.empty or json.empty:
            raise ValueError('Found only one of pgn/json empty for input '
                             f'{self.player=} {self.perf_type=} {self.since=} '
                             f'{self.single_day=}')

        df = clean_chess_df(pgn, json)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class GetEvals(Task):

    local_stockfish = BoolParameter()

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-game-evals-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        stockfish_params = stockfish_cfg()

        df = df[['evaluations', 'eval_depths', 'positions']]

        # explode the two different list-likes separately, then concat
        no_evals = df[~df['evaluations'].astype(bool)]
        df = df[df['evaluations'].astype(bool)]

        no_evals = pd.DataFrame(no_evals['positions'].explode())
        no_evals['positions'] = get_clean_fens(no_evals['positions'])

        evals = df['evaluations'].explode().reset_index(drop=True)
        depths = df['eval_depths'].explode().reset_index(drop=True)
        positions = df['positions'].explode().reset_index(drop=True)
        positions = get_clean_fens(positions)

        sql = """SELECT fen, evaluation, eval_depth
                 FROM position_evals
                 WHERE fen IN %(positions)s;
                 """
        db_evaluations = run_remote_sql_query(sql,
                                              positions=tuple(positions.tolist() + no_evals['positions'].tolist()),  # noqa
                                              )
        positions_evaluated = db_evaluations['fen'].drop_duplicates()

        df = pd.concat([positions, evals, depths], axis=1)

        if self.local_stockfish:

            local_evals = []

            counter = 0
            position_count = len(no_evals['positions'])
            evaluation = None

            for position in no_evals['positions'].tolist():
                if position in positions_evaluated.values:
                    # position will be dropped later if evaluation is None
                    evaluation = None
                else:
                    sf_eval = get_sf_evaluation(position + ' 0',
                                                stockfish_params.location,
                                                stockfish_params.depth)
                    if sf_eval is not None:
                        # TODO: this is implicitly setting evaluation = last
                        # eval if in a checkmate position. handle this better
                        evaluation = sf_eval

                local_evals.append(evaluation)

                # progress bar stuff
                counter += 1

                current_progress = counter / position_count
                self.set_status_message(f'Analyzed :: '
                                        f'{counter} / {position_count}')
                self.set_progress_percentage(round(current_progress * 100, 2))

            self.set_status_message(f'Analyzed all {position_count} positions')
            self.set_progress_percentage(100)

            no_evals['evaluations'] = local_evals
            no_evals['eval_depths'] = stockfish_params.depth
            no_evals.dropna(inplace=True)

            df = pd.concat([df, no_evals], axis=0, ignore_index=True)

        df = df[~df['positions'].isin(positions_evaluated)]

        df.rename(columns={'evaluations': 'evaluation',
                           'eval_depths': 'eval_depth',
                           'positions': 'fen'},
                  inplace=True)
        df['evaluation'] = pd.to_numeric(df['evaluation'],
                                         errors='coerce')

        df.dropna(inplace=True)
        df = pd.concat([df, db_evaluations], axis=0, ignore_index=True)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodeMoves(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-game-moves-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        df = df[['game_link', 'moves']]

        df = df.explode('moves')
        df.rename(columns={'moves': 'move'},
                  inplace=True)
        df['half_move'] = df.groupby('game_link').cumcount() + 1

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodeClocks(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-game-clocks-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        df = df[['game_link', 'clocks']]

        df = df.explode('clocks')
        df.rename(columns={'clocks': 'clock'},
                  inplace=True)
        df['half_move'] = df.groupby('game_link').cumcount() + 1
        df['clock'] = convert_clock_to_seconds(df['clock'])

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodePositions(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-game-positions-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        df = df[['game_link', 'positions']]

        df = df.explode('positions')
        df.rename(columns={'positions': 'position'},
                  inplace=True)
        df['half_move'] = df.groupby('game_link').cumcount() + 1

        df['fen'] = get_clean_fens(df['position'])

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodeMaterials(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-game-materials-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

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

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class GetGameInfos(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-game-infos-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        df = transform_game_data(df, self.player)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(GetEvals, ExplodePositions, ExplodeClocks, GetGameInfos)
class EstimateWinProbabilities(Task):

    def output(self):
        file_location = (f'~/Temp/luigi/{self.since}-win-probs-'
                         f'{self.player}-{self.perf_type}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        self.output().makedirs()

        with self.input()[0].open('r') as f:
            evals = pd.read_pickle(f, compression=None)

        with self.input()[1].open('r') as f:
            game_positions = pd.read_pickle(f, compression=None)

        with self.input()[2].open('r') as f:
            game_clocks = pd.read_pickle(f, compression=None)

        with self.input()[3].open('r') as f:
            game_infos = pd.read_pickle(f, compression=None)

        if game_infos.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                game_infos.to_pickle(temp_output_path, compression=None)

            return

        game_infos['has_increment'] = (game_infos['increment'] > 0).astype(int)

        game_infos_cols = ['game_link',
                           'has_increment',
                           'player_color',
                           'player_elo',
                           'opponent_elo',
                           ]

        # evals isn't always populated
        df = pd.merge(game_positions, evals, on='fen', how='left')

        # if there are missing evals, set to 0 so it doesn't influence the WP
        if not self.local_stockfish:
            df['evaluation'].fillna(0, inplace=True)
            # this is actually kind of incorrect - evaluation was never scaled
            # so the mean isn't 0, but rather something like 0.2 probably.
            # since the LR model inputs weren't scaled in the first place,
            # i am just ignoring this for now

        df = pd.merge(df, game_clocks, on=['game_link', 'half_move'])
        df = pd.merge(df,
                      game_infos[game_infos_cols],
                      on='game_link',
                      )

        loss, draw, win = predict_wp(df)

        df['win_probability_white'] = win
        df['draw_probability'] = draw
        df['win_probability_black'] = loss

        model_path = os.path.join(os.path.dirname(__file__),
                                  'pipeline_import',
                                  'wp_model.pckl',
                                  )

        with open(model_path, 'rb') as f:
            md5 = hashlib.md5(f.read()).hexdigest()

        df['win_prob_model_version'] = md5[:7]

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(GetGameInfos)
class ChessGames(TransactionFactTable):
    pass


@requires(GetEvals)
class PositionEvals(TransactionFactTable):
    pass


@requires(ExplodePositions)
class GamePositions(TransactionFactTable):
    pass


@requires(ExplodeMaterials)
class GameMaterials(TransactionFactTable):
    pass


@requires(ExplodeClocks)
class MoveClocks(TransactionFactTable):
    pass


@requires(ExplodeMoves)
class MoveList(TransactionFactTable):
    pass


@requires(EstimateWinProbabilities)
class WinProbs(TransactionFactTable):
    pass


@inherits(FetchLichessApiPGN, ChessGames, MoveClocks, GetEvals)
class CopyGames(CopyWrapper):

    jobs = [{'table_type': ChessGames,
             'fn':         GetGameInfos,
             'table':      'chess_games',
             'columns':    ['event_type',
                            'result',
                            'round',
                            'game_link',
                            'termination',
                            'chess_variant',
                            'black_elo_tentative',
                            'white_elo_tentative',
                            'player',
                            'opponent',
                            'player_color',
                            'opponent_color',
                            'player_rating_diff',
                            'opponent_rating_diff',
                            'player_result',
                            'opponent_result',
                            'time_control_category',
                            'datetime_played',
                            'starting_time',
                            'increment',
                            'in_arena',
                            'rated_casual',
                            'player_elo',
                            'opponent_elo',
                            'queen_exchange',
                            'player_castling_side',
                            'opponent_castling_side',
                            'lichess_opening',
                            'opening_played',
                            'has_promotion',
                            'promotion_count_white',
                            'promotion_count_black',
                            'promotions_white',
                            'promotions_black',
                            'black_berserked',
                            'white_berserked',
                            ],
             'id_cols':    ['player',
                            'game_link'],
             'date_cols':  ['datetime_played'],
             'merge_cols': HashableDict()},
            {'table_type': PositionEvals,
             'fn': GetEvals,
             'table': 'position_evals',
             'columns': ['fen',
                         'evaluation',
                         'eval_depth',
                         ],
             'id_cols': ['fen'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': MoveClocks,
             'fn': ExplodeClocks,
             'table': 'game_clocks',
             'columns': ['game_link',
                         'half_move',
                         'clock',
                         ],
             'id_cols': ['game_link',
                         'half_move'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': GamePositions,
             'fn': ExplodePositions,
             'table': 'game_positions',
             'columns': ['game_link',
                         'half_move',
                         'fen',
                         ],
             'id_cols': ['game_link',
                         'half_move'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': MoveList,
             'fn': ExplodeMoves,
             'table': 'game_moves',
             'columns': ['game_link',
                         'half_move',
                         'move',
                         ],
             'id_cols': ['game_link',
                         'half_move'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': GameMaterials,
             'fn': ExplodeMaterials,
             'table': 'game_materials',
             'columns': ['game_link',
                         'half_move',
                         'pawns_white',
                         'pawns_black',
                         'bishops_white',
                         'bishops_black',
                         'knights_white',
                         'knights_black',
                         'rooks_white',
                         'rooks_black',
                         'queens_white',
                         'queens_black',
                         ],
             'id_cols': ['game_link',
                         'half_move'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            {'table_type': WinProbs,
             'fn': EstimateWinProbabilities,
             'table': 'win_probabilities',
             'columns': ['game_link',
                         'half_move',
                         'win_probability_white',
                         'draw_probability',
                         'win_probability_black',
                         'win_prob_model_version',
                         ],
             'id_cols': ['game_link',
                         'half_move'],
             'date_cols': [],
             'merge_cols': HashableDict()},
            ]

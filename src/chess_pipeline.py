#! /usr/bin/env python3

import os
from datetime import datetime, timedelta

import pandas as pd
from feature_engineering import (
    clean_chess_df,
    explode_clocks,
    explode_materials,
    explode_moves,
    explode_positions,
)
from inference import estimate_win_probabilities
from luigi import LocalTarget, Task
from luigi.format import Nop
from luigi.parameter import BoolParameter, DateParameter, Parameter
from luigi.util import inherits, requires
from pipeline_import.postgres_templates import (
    CopyWrapper,
    HashableDict,
    TransactionFactTable,
)
from pipeline_import.transforms import transform_game_data
from vendors.lichess import fetch_lichess_api_json, fetch_lichess_api_pgn
from vendors.stockfish import get_evals


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

        df: pd.DataFrame = get_evals(df, self.local_stockfish, self)

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

        df = explode_moves(df)

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

        df = explode_clocks(df)

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

        df = explode_positions(df)

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

        df = explode_materials(df)

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

        df = estimate_win_probabilities(game_infos,
                                        evals,
                                        game_positions,
                                        game_clocks,
                                        self.local_stockfish,
                                        )

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

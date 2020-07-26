#! /usr/bin/env python3

from luigi.parameter import Parameter, ListParameter, BoolParameter
from luigi.parameter import DateParameter
import psycopg2
from luigi.util import requires, inherits
from luigi.format import Nop
from luigi import Task, LocalTarget
from pandas import DataFrame
from pipeline_import.postgres_templates import CopyWrapper, HashableDict
from pipeline_import.postgres_templates import TransactionFactTable
from datetime import datetime, timedelta
from pipeline_import.configs import lichess_token, stockfish_cfg, postgres_cfg


def query_for_column(table, column):
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

    cursor = db.cursor()
    sql = """SELECT DISTINCT {} FROM {};""".format(column, table)

    cursor.execute(sql)

    result = cursor.fetchall()

    current_srs = DataFrame(result)

    if current_srs.empty:
        current_srs = DataFrame([0])
    return current_srs[0]


class FetchLichessApiPGN(Task):

    player = Parameter(default='thibault')
    perf_type = Parameter(default='blitz')
    since = DateParameter(default=datetime.today().date() - timedelta(days=1))
    single_day = BoolParameter()
    local_stockfish = BoolParameter()

    def output(self):
        import os

        file_location = '~/Temp/luigi/raw-games-%s-pgn.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import lichess.api
        from lichess.format import PYCHESS
        from pandas import DataFrame
        from calendar import timegm
        from pipeline_import.visitors import EvalsVisitor, ClocksVisitor
        from pipeline_import.visitors import QueenExchangeVisitor
        from pipeline_import.visitors import CastlingVisitor, StockfishVisitor
        from pipeline_import.visitors import PromotionsVisitor

        self.output().makedirs()

        if self.single_day:
            unix_time_until = timegm((self.since
                                      + timedelta(days=1)).timetuple())
        else:
            unix_time_until = timegm(datetime.today().date().timetuple())
        self.until = int(1000 * unix_time_until)

        unix_time_since = timegm(self.since.timetuple())
        self.since = int(1000 * unix_time_since)

        token = lichess_token().token
        stockfish_params = stockfish_cfg()

        games = lichess.api.user_games(self.player,
                                       since=self.since,
                                       until=self.until,
                                       perfType=self.perf_type,
                                       auth=token,
                                       clocks='true',
                                       evals='true',
                                       opening='true',
                                       format=PYCHESS)

        visitors = [EvalsVisitor,
                    ClocksVisitor,
                    QueenExchangeVisitor,
                    CastlingVisitor,
                    PromotionsVisitor,
                    ]

        visitor_stats = {'clocks': 'clocks',
                         'evaluations': 'evals',
                         'eval_depth': 'eval_depth',
                         'queen_exchange': 'queen_exchange',
                         'castling_sides': 'castling',
                         'has_promotion': 'has_promotion',
                         'promotion_count': 'promotion_count',
                         'promoted_to': 'promotions',
                         }

        header_infos = []

        counter = 0
        total_time = self.until - self.since

        evals_finished = query_for_column('game_evals', 'game_link')

        for game in games:
            game_infos = {x: y for x, y in game.headers.items()}
            if game.headers['Variant'] == 'From Position':
                game.headers['Variant'] = 'Standard'
            for visitor in visitors:
                game.accept(visitor(game))
            eval_done = evals_finished.isin([game.headers['Site']]).any()
            if not any(game.evals) and self.local_stockfish and not eval_done:
                game.accept(StockfishVisitor(game,
                                             stockfish_params.location,
                                             stockfish_params.depth))
                # adjust for centipawn scale
                game.evals = [x / 100 for x in game.evals]
            for k, v in visitor_stats.items():
                game_infos[k] = getattr(game, v)
            game_infos['moves'] = [x.san() for x in game.mainline()]
            header_infos.append(game_infos)

            # progress bar stuff
            counter += 1

            if counter % 5 == 0:
                current = '{} {}'.format(game_infos['UTCDate'],
                                         game_infos['UTCTime'])
                time_parsed = datetime.strptime(current,
                                                '%Y.%m.%d %H:%M:%S')
                unix_time_parsed = timegm(time_parsed.timetuple())
                current_unix = int(unix_time_parsed * 1000)

                current_progress = (self.until - current_unix) / total_time
                self.set_status_message('Parsed until {}'.format(current))
                self.set_progress_percentage(round(current_progress * 100, 2))

        df = DataFrame(header_infos)

        self.set_status_message('Parsed all games')
        self.set_progress_percentage(100)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


class FetchLichessApiJSON(Task):

    player = Parameter(default='thibault')
    perf_type = Parameter(default='blitz')
    since = DateParameter(default=datetime.today().date() - timedelta(days=1))
    single_day = BoolParameter()

    def output(self):
        import os

        file_location = '~/Temp/luigi/raw-games-%s-json.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import lichess.api
        from lichess.format import JSON
        from pandas.io.json import json_normalize
        from calendar import timegm

        self.output().makedirs()

        if self.single_day:
            unix_time_until = timegm((self.since
                                      + timedelta(days=1)).timetuple())
        else:
            unix_time_until = timegm(datetime.today().date().timetuple())
        self.until = int(1000 * unix_time_until)

        unix_time_since = timegm(self.since.timetuple())
        self.since = int(1000 * unix_time_since)

        token = lichess_token().token

        games = lichess.api.user_games(self.player,
                                       since=self.since,
                                       until=self.until,
                                       perfType=self.perf_type,
                                       auth=token,
                                       evals='true',
                                       moves='false',
                                       format=JSON)

        df = json_normalize([game
                             for game in games],
                            sep='_')

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(FetchLichessApiPGN)
class CleanChessDF(Task):

    def output(self):
        import os

        file_location = '~/Temp/luigi/cleaned-games-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        # rename columns
        df.rename(columns={'Black':           'black',
                           'BlackElo':        'black_elo',
                           'BlackRatingDiff': 'black_rating_diff',
                           'Date':            'date_played',
                           'ECO':             'opening_played',
                           'Event':           'event_type',
                           'Result':          'result',
                           'Round':           'round',
                           'Site':            'game_link',
                           'Termination':     'termination',
                           'TimeControl':     'time_control',
                           'UTCDate':         'utc_date_played',
                           'UTCTime':         'time_played',
                           'Variant':         'chess_variant',
                           'White':           'white',
                           'WhiteElo':        'white_elo',
                           'WhiteRatingDiff': 'white_rating_diff',
                           'Opening':         'lichess_opening'
                           },
                  inplace=True)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodeEvals(Task):

    columns = ListParameter()

    def output(self):
        import os

        file_location = '~/Temp/luigi/game-evals-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle, to_numeric

        self.output().makedirs()

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        df = df[['game_link', 'evaluations', 'eval_depth']]

        df = df.explode('evaluations')
        df.rename(columns={'evaluations': 'evaluation'},
                  inplace=True)
        df['half_move'] = df.groupby('game_link').cumcount() + 1
        df['evaluation'] = to_numeric(df['evaluation'],
                                      errors='coerce')

        df.dropna(inplace=True)

        df = df[list(self.columns)]

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodeMoves(Task):

    columns = ListParameter()

    def output(self):
        import os

        file_location = '~/Temp/luigi/game-moves-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

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

        df = df[list(self.columns)]

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class ExplodeClocks(Task):

    columns = ListParameter()

    def output(self):
        import os

        file_location = '~/Temp/luigi/game-clocks-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle, to_timedelta

        self.output().makedirs()

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

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
        df['clock'] = to_timedelta(df['clock'],
                                   errors='coerce')
        df['clock'] = df['clock'].dt.total_seconds()
        df['clock'].fillna(-1.0, inplace=True)
        df['clock'] = df['clock'].astype(int)

        df = df[list(self.columns)]

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class GetGameInfos(Task):

    columns = ListParameter()

    def output(self):
        import os

        file_location = '~/Temp/luigi/game-infos-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle, to_datetime, to_numeric
        from pandas import concat, Series, merge

        self.output().makedirs()

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        # add new columns
        for column in ['black_elo', 'white_elo']:
            df[column + '_tentative'] = 'Unknown'

        df['player'] = self.player

        if 'black_rating_diff' not in df.columns:
            df['black_rating_diff'] = 0

        if 'white_rating_diff' not in df.columns:
            df['white_rating_diff'] = 0

        # add two strings and remove the player name so that we don't
        # have to use pd.DataFrame.apply
        df['opponent'] = df['white'] + df['black']
        df['opponent'] = df['opponent'].str.replace(self.player, '')

        series_player_black = df['black'] == self.player
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

        df['time_control_category'] = self.perf_type
        df['datetime_played'] = to_datetime(df['utc_date_played'].astype(str)
                                            + ' '
                                            + df['time_played'].astype(str))
        df['starting_time'] = df['time_control'].str.extract(r'(\d+)\+')
        df['increment'] = df['time_control'].str.extract(r'\+(\d+)')

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

        # filter unnecessary columns out
        df = df[list(self.columns)]

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(GetGameInfos)
class ChessGames(TransactionFactTable):
    pass


@requires(ExplodeEvals)
class MoveEvals(TransactionFactTable):
    pass


@requires(ExplodeClocks)
class MoveClocks(TransactionFactTable):
    pass


@requires(ExplodeMoves)
class MoveList(TransactionFactTable):
    pass


@inherits(FetchLichessApiPGN, ChessGames, MoveEvals)
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
                            'promotion_count',
                            'promotions',
                            ],
             'id_cols':    ['player',
                            'game_link'],
             'date_cols':  ['datetime_played'],
             'merge_cols': HashableDict()},
            {'table_type': MoveEvals,
             'fn': ExplodeEvals,
             'table': 'game_evals',
             'columns': ['game_link',
                         'half_move',
                         'evaluation',
                         'eval_depth',
                         ],
             'id_cols': ['game_link',
                         'half_move'],
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
            ]

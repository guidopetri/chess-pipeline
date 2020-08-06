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
import stockfish
import re


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
    sql = f"""SELECT DISTINCT {column} FROM {table};"""

    cursor.execute(sql)

    result = cursor.fetchall()

    current_srs = DataFrame(result)

    if current_srs.empty:
        current_srs = DataFrame([0])
    return current_srs[0]


def get_sf_evaluation(fen, sf_location, sf_depth):
    sf = stockfish.Stockfish(sf_location,
                             depth=sf_depth)

    sf.set_fen_position(fen)
    if sf.get_best_move() is not None:
        rating_match = re.search(r'score (cp|mate) (.+?)(?: |$)',
                                 sf.info)

        if rating_match.group(1) == 'mate':
            original_rating = int(rating_match.group(2))

            # adjust ratings for checkmate sequences
            if original_rating:
                rating = 999900 * original_rating / abs(original_rating)
            elif ' w ' in fen:
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


class FetchLichessApiJSON(Task):

    player = Parameter(default='thibault')
    perf_type = Parameter(default='blitz')
    since = DateParameter(default=datetime.today().date() - timedelta(days=1))
    single_day = BoolParameter()

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/{self.since}-raw-games-'
                         f'{self.player}-json.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import lichess.api
        from lichess.format import JSON
        from pandas import json_normalize
        from calendar import timegm

        self.output().makedirs()

        if self.single_day:
            unix_time_until = timegm((self.since
                                      + timedelta(days=1)).timetuple())
        else:
            unix_time_until = timegm(datetime.today().date().timetuple())
        self.until = int(1000 * unix_time_until)

        unix_time_since = timegm(self.since.timetuple())
        self.since_unix = int(1000 * unix_time_since)

        token = lichess_token().token

        games = lichess.api.user_games(self.player,
                                       since=self.since_unix,
                                       until=self.until,
                                       perfType=self.perf_type,
                                       auth=token,
                                       evals='false',
                                       clocks='false',
                                       moves='false',
                                       format=JSON)

        df = json_normalize([game
                             for game in games],
                            sep='_')

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(FetchLichessApiJSON)
class FetchLichessApiPGN(Task):

    player = Parameter(default='thibault')
    perf_type = Parameter(default='blitz')
    since = DateParameter(default=datetime.today().date() - timedelta(days=1))
    single_day = BoolParameter()

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/{self.since}-raw-games-'
                         f'{self.player}-pgn.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import lichess.api
        from lichess.format import PYCHESS
        from pandas import DataFrame, read_pickle
        from calendar import timegm
        from pipeline_import.visitors import EvalsVisitor, ClocksVisitor
        from pipeline_import.visitors import QueenExchangeVisitor
        from pipeline_import.visitors import CastlingVisitor, PositionsVisitor
        from pipeline_import.visitors import PromotionsVisitor

        self.output().makedirs()

        with self.input().open('r') as f:
            json = read_pickle(f, compression=None)
            game_count = len(json)

        if self.single_day:
            unix_time_until = timegm((self.since
                                      + timedelta(days=1)).timetuple())
        else:
            unix_time_until = timegm(datetime.today().date().timetuple())
        self.until = int(1000 * unix_time_until)

        unix_time_since = timegm(self.since.timetuple())
        self.since_unix = int(1000 * unix_time_since)

        token = lichess_token().token

        games = lichess.api.user_games(self.player,
                                       since=self.since_unix,
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
                    PositionsVisitor,
                    ]

        visitor_stats = {'clocks': 'clocks',
                         'evaluations': 'evals',
                         'eval_depths': 'eval_depths',
                         'queen_exchange': 'queen_exchange',
                         'castling_sides': 'castling',
                         'has_promotion': 'has_promotion',
                         'promotion_count_white': 'promotion_count_white',
                         'promotion_count_black': 'promotion_count_black',
                         'promotions_white': 'promotions_white',
                         'promotions_black': 'promotions_black',
                         'positions': 'positions',
                         }

        header_infos = []

        counter = 0

        for game in games:
            game_infos = {x: y for x, y in game.headers.items()}
            if game.headers['Variant'] == 'From Position':
                game.headers['Variant'] = 'Standard'
            for visitor in visitors:
                game.accept(visitor(game))
            for k, v in visitor_stats.items():
                game_infos[k] = getattr(game, v)
            game_infos['moves'] = [x.san() for x in game.mainline()]
            header_infos.append(game_infos)

            # progress bar stuff
            counter += 1

            current = f'{game_infos["UTCDate"]} {game_infos["UTCTime"]}'

            current_progress = counter / game_count
            self.set_status_message(f'Parsed until {current} :: '
                                    f'{counter} / {game_count}')
            self.set_progress_percentage(round(current_progress * 100, 2))

        df = DataFrame(header_infos)

        self.set_status_message('Parsed all games')
        self.set_progress_percentage(100)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(FetchLichessApiPGN, FetchLichessApiJSON)
class CleanChessDF(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/{self.since}-cleaned-games-'
                         f'{self.player}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle, merge

        self.output().makedirs()

        with self.input()[0].open('r') as f:
            pgn = read_pickle(f, compression=None)

        with self.input()[1].open('r') as f:
            json = read_pickle(f, compression=None)

        # hopefully, if pgn is empty so is json
        if pgn.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                pgn.to_pickle(temp_output_path, compression=None)

            return

        json['Site'] = 'https://lichess.org/' + json['id']
        json = json[['Site', 'speed', 'status']]

        df = merge(pgn, json, on='Site')

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
class GetEvals(Task):

    local_stockfish = BoolParameter()
    columns = ListParameter()

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/{self.since}-game-evals-'
                         f'{self.player}.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle, to_numeric, concat, DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().temporary_path() as temp_output_path:
                df.to_pickle(temp_output_path, compression=None)

            return

        stockfish_params = stockfish_cfg()

        df = df[['evaluations', 'eval_depths', 'positions']]

        positions_evaluated = query_for_column('position_evals', 'fen')

        # explode the two different list-likes separately, then concat
        no_evals = df[~df['evaluations'].astype(bool)]
        df = df[df['evaluations'].astype(bool)]

        evals = df['evaluations'].explode().reset_index(drop=True)
        depths = df['eval_depths'].explode().reset_index(drop=True)
        positions = df['positions'].explode().reset_index(drop=True)
        positions = positions.str.split().str[:-1].str.join(' ')

        df = concat([positions, evals, depths], axis=1)

        if self.local_stockfish:
            no_evals = DataFrame(no_evals['positions'].explode())
            no_evals['positions'] = (no_evals['positions'].str.split()
                                                          .str[:-1]
                                                          .str.join(' '))

            local_evals = []

            counter = 0
            position_count = len(no_evals['positions'])

            for position in no_evals['positions'].tolist():
                if position in positions_evaluated.values:
                    evaluation = None
                else:
                    evaluation = (get_sf_evaluation(position + ' 0',
                                                    stockfish_params.location,
                                                    stockfish_params.depth)
                                  or evaluation)
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

            df = concat([df, no_evals], axis=0, ignore_index=True)

        df = df[~df['positions'].isin(positions_evaluated)]

        df.rename(columns={'evaluations': 'evaluation',
                           'eval_depths': 'eval_depth',
                           'positions': 'fen'},
                  inplace=True)
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

        file_location = (f'~/Temp/luigi/{self.since}-game-moves-'
                         f'{self.player}.pckl')
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

        file_location = (f'~/Temp/luigi/{self.since}-game-clocks-'
                         f'{self.player}.pckl')
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
class ExplodePositions(Task):

    columns = ListParameter()

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/{self.since}-game-positions-'
                         f'{self.player}.pckl')
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

        df = df[['game_link', 'positions']]

        df = df.explode('positions')
        df.rename(columns={'positions': 'position'},
                  inplace=True)
        df['half_move'] = df.groupby('game_link').cumcount() + 1

        # split, get all but last element of resulting list, then re-join
        df['fen'] = df['position'].str.split().str[:-1].str.join(' ')

        df = df[list(self.columns)]

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)


@requires(CleanChessDF)
class GetGameInfos(Task):

    columns = ListParameter()

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/{self.since}-game-infos-'
                         f'{self.player}.pckl')
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

        df.rename(columns={'speed': 'time_control_category'},
                  inplace=True)

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


@requires(GetEvals)
class PositionEvals(TransactionFactTable):
    pass


@requires(ExplodePositions)
class GamePositions(TransactionFactTable):
    pass


@requires(ExplodeClocks)
class MoveClocks(TransactionFactTable):
    pass


@requires(ExplodeMoves)
class MoveList(TransactionFactTable):
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
            ]

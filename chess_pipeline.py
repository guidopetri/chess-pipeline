#! /usr/bin/env python3

from luigi.parameter import Parameter, ListParameter, IntParameter
from luigi.parameter import DateParameter
from luigi.util import requires, inherits
from luigi.format import Nop
from luigi import Task
from luigi.mock import MockTarget
from postgres_templates import CopyWrapper, HashableDict, TransactionFactTable
from datetime import datetime, timedelta


class FetchLichessApi(Task):

    date = DateParameter(default=datetime.today())
    player = Parameter(default='thibault')
    perfType = Parameter(default='blitz')
    since = IntParameter(default=None)

    def output(self):
        return MockTarget('LichessGames %s' % self.player, format=Nop)

    def run(self):
        import lichess.api
        from lichess.format import PYCHESS
        from pandas import DataFrame

        if self.since is None:
            two_days_ago = datetime.today() - timedelta(days=2)
            unix_time = two_days_ago.timestamp()
            self.since = int(1000 * unix_time)

        games = lichess.api.user_games(self.player,
                                       since=self.since,
                                       perfType=self.perfType,
                                       format=PYCHESS)

        header_infos = []
        for game in games:
            header_infos.append({x: y for x, y in game.headers.items()})

        df = DataFrame(header_infos)

        with self.output().open('w') as f:
            df.to_pickle(f, compression=None)


@requires(FetchLichessApi)
class CleanChessDF(Task):

    columns = ListParameter()

    def output(self):
        return MockTarget('CleanChessDF', format=Nop)

    def run(self):
        from pandas import read_pickle, to_datetime, to_numeric
        from numpy import nan

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:

            def complete(self):
                return True

            with self.output().open('w') as db:
                df.to_pickle(db, compression=None)

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
                           },
                  inplace=True)

        # add new columns
        for column in ['black_elo', 'white_elo']:
            df[column + '_tentative'] = df[column].str.contains(r'\?')

        df['player'] = self.player
        series_player_black = df['black'] == self.player
        df['player_color'] = series_player_black.map({True: 'black',
                                                      False: 'white'
                                                      })
        df['player_rating_diff'] = ((series_player_black
                                     * df['black_rating_diff'])
                                    + (~series_player_black
                                        * df['white_rating_diff']))

        series_result_helper = df['result'] + series_player_black.astype(str)
        df['player_result'] = series_result_helper.map({'0-1True': 'Win',
                                                        '1-0False': 'Win',
                                                        '1/2-1/2True': 'Draw',
                                                        '1/2-1/2False': 'Draw',
                                                        '1-0True': 'Loss',
                                                        '0-1False': 'Loss'})

        df['time_control_category'] = self.perfType
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

        # type handling
        df['date_played'] = to_datetime(df['date_played'])
        df['utc_date_played'] = to_datetime(df['utc_date_played'])

        rating_columns = ['black_elo',
                          'black_rating_diff',
                          'white_elo',
                          'white_rating_diff',
                          ]

        for column in rating_columns:
            df[column] = df[column].str.replace('?', '')
            df[column] = df[column].replace('', nan)
            df[column].fillna(0, inplace=True)
            df[column] = to_numeric(df[column])

        # filter unnecessary columns out
        df = df[list(self.columns)]

        with self.output().open('w') as db:
            df.to_pickle(db, compression=None)


@requires(CleanChessDF)
class ChessGames(TransactionFactTable):

    pass


@inherits(FetchLichessApi, ChessGames)
class CopyGames(CopyWrapper):

    jobs = [{'table_type': ChessGames,
             'fn':         CleanChessDF,
             'table':      'chess_games',
             'columns':    ['black',
                            'black_elo',
                            'black_rating_diff',
                            'date_played',
                            'opening_played',
                            'event_type',
                            'result',
                            'round',
                            'game_link',
                            'termination',
                            'time_control',
                            'utc_date_played',
                            'time_played',
                            'chess_variant',
                            'white',
                            'white_elo',
                            'white_rating_diff',
                            ],
             'id_col':     'game_link',
             'date_cols':  ['date_played', 'utc_date_played'],
             'merge_cols': HashableDict()},
            ]

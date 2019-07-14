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

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if not df.empty:
            df['Date'] = to_datetime(df['Date'])
            df['UTCDate'] = to_datetime(df['UTCDate'])

            rating_columns = ['BlackElo',
                              'BlackRatingDiff',
                              'WhiteElo',
                              'WhiteRatingDiff',
                              ]

            for column in rating_columns:
                df[column] = df[column].replace('?', '')
                df[column] = to_numeric(df[column])

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

#! /usr/bin/env python3

from luigi.parameter import Parameter, ListParameter
from luigi.parameter import DateParameter, ParameterVisibility
from luigi.util import requires, inherits
from luigi.format import Nop
from luigi import Task, LocalTarget
from postgres_templates import CopyWrapper, HashableDict, TransactionFactTable
from datetime import datetime, timedelta


class FetchLichessApi(Task):

    today = datetime.today()

    player = Parameter(default='thibault')
    perfType = Parameter(default='blitz')
    lichess_token = Parameter(visibility=ParameterVisibility.PRIVATE,
                              significant=False)
    since = DateParameter(default=today - timedelta(days=1))
    until = DateParameter(default=today)

    def output(self):
        import os

        file_location = '~/Temp/luigi/raw-games-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import lichess.api
        from lichess.format import PYCHESS
        from pandas import DataFrame
        from calendar import timegm

        self.output().makedirs()

        unix_time_prev = timegm(self.since.timetuple())
        self.since = int(1000 * unix_time_prev)

        unix_time_until = timegm(self.until.timetuple())
        self.until = int(1000 * unix_time_until)

        games = lichess.api.user_games(self.player,
                                       since=self.since,
                                       until=self.until,
                                       perfType=self.perfType,
                                       auth=self.lichess_token,
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
        import os

        file_location = '~/Temp/luigi/cleaned-games-%s.pckl' % self.player
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        from pandas import read_pickle, to_datetime, to_numeric

        self.output().makedirs()

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
            df[column + '_tentative'] = 'Unknown'

        df['player'] = self.player

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
            # ? ratings are anonymous players
            df[column] = df[column].replace('?', '1500')
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
                            ],
             'id_cols':    ['player',
                            'game_link'],
             'date_cols':  ['date_played', 'utc_date_played'],
             'merge_cols': HashableDict()},
            ]

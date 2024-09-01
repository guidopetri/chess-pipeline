#! /usr/bin/env python3

import os
import pickle

import numpy as np
import numpy.typing as npt
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy


def load_win_probability_model():
    file_path: str = os.path.join(os.path.dirname(__file__), 'wp_model.pckl')
    with open(file_path, 'rb') as f:
        model = pickle.load(f)
    return model


def create_wp_features(df: pd.DataFrame) -> pd.DataFrame:
    df.sort_values(by=['game_link', 'half_move'], ascending=True, inplace=True)

    # filter out where we don't have clock times
    df = df[df['clock'] != -1]

    df['opponent_clock'] = df.groupby(['game_link'])['clock'].shift(-1)
    df['opponent_clock'] = df['opponent_clock'].fillna(df['opponent_clock'].shift(2))  # noqa

    # in situations where there were only one or two moves,
    # fill with the clock time
    df['opponent_clock'] = df['opponent_clock'].fillna(df['clock'])

    # start with white
    df['player_to_move'] = df['half_move'] % 2

    # group by game and player; sometimes players have different clock times
    # (e.g. berserk in arena)
    initial_times_groupby: DataFrameGroupBy = df.groupby(['game_link',
                                                          'player_to_move'])
    # get only first row of the columns we need
    initial_times = initial_times_groupby[['game_link',
                                           'player_to_move',
                                           'clock']].head(1)
    initial_times.columns = ['game_link', 'player_to_move', 'initial_clock']
    df = pd.merge(df,
                  initial_times,
                  on=['game_link', 'player_to_move'],
                  how='inner',
                  )

    # flip bit for opponent's time to move
    initial_times['player_to_move'] = (initial_times['player_to_move'] + 1) % 2
    initial_times.columns = ['game_link',
                             'player_to_move',
                             'opponent_initial_clock',
                             ]
    df = pd.merge(df,
                  initial_times,
                  on=['game_link', 'player_to_move'],
                  how='inner',
                  )

    # set min time to 1, max time to initial time
    df['clock_pct'] = (np.clip(df['clock'], a_min=1, a_max=None)
                       / df['initial_clock'])
    df['clock_pct'] = np.clip(df['clock_pct'], a_min=None, a_max=1)

    # reverse sigmoid transform
    df['sig_clock_pct'] = np.log(df['clock_pct'] / (1.00001 - df['clock_pct']))

    # same for opponent
    df['opponent_clock_pct'] = (np.clip(df['opponent_clock'],
                                        a_min=1,
                                        a_max=None)
                                / df['opponent_initial_clock'])
    df['opponent_clock_pct'] = np.clip(df['opponent_clock_pct'],
                                       a_min=None,
                                       a_max=1,
                                       )
    df['opponent_sig_clock_pct'] = (np.log(df['opponent_clock_pct']
                                    / (1.00001 - df['opponent_clock_pct'])))

    # map to white/black colors
    df['player_color_mapped'] = df['player_color'].map({'black': 1,
                                                        'white': 0})

    df['white_sig_clock_pct'] = (df['player_color_mapped'] * df['opponent_sig_clock_pct']  # noqa
                                 + (1 - df['player_color_mapped']) * df['sig_clock_pct'])  # noqa
    df['black_sig_clock_pct'] = (df['player_color_mapped'] * df['sig_clock_pct']  # noqa
                                 + (1 - df['player_color_mapped']) * df['opponent_sig_clock_pct'])  # noqa

    # create elo diff
    df['white_elo'] = (df['player_color_mapped'] * df['opponent_elo']
                       + (1 - df['player_color_mapped']) * df['player_elo'])
    df['black_elo'] = (df['player_color_mapped'] * df['player_elo']
                       + (1 - df['player_color_mapped']) * df['opponent_elo'])

    df['elo_diff'] = df['white_elo'] - df['black_elo']

    return df


def predict_wp(df: pd.DataFrame,
               ) -> tuple[npt.NDArray[np.float64],
                          npt.NDArray[np.float64],
                          npt.NDArray[np.float64]]:

    model = load_win_probability_model()

    # create features
    df = create_wp_features(df)

    cols = ['elo_diff',
            'evaluation',
            'white_sig_clock_pct',
            'black_sig_clock_pct',
            'has_increment',
            ]

    probs: npt.NDArray[np.float64] = model.predict_proba(df[cols]).round(6)

    return probs[:, 0], probs[:, 1], probs[:, 2]

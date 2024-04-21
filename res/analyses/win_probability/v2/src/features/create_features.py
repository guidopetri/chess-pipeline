#! /usr/bin/env python

import numpy as np
import pandas as pd

pd.options.display.max_columns = 999

df = pd.read_csv('../../data/raw/dataset.csv')

df = df[df['evaluation'].notna()]

df['result_points'] = df['result'].map({'1-0': 1,
                                        '1/2-1/2': 0.5,
                                        '0-1': 0})
df['evaluation_binned'] = (df['evaluation'] / 3).round(decimals=0) * 3

df.sort_values(by=['game_link', 'half_move'], ascending=True, inplace=True)

# filter out where we don't have clock times
df = df[df['clock'] != -1]

df['opponent_clock'] = df.groupby(['game_link'])['clock'].shift(-1)
df['opponent_clock'].fillna(df['opponent_clock'].shift(2), inplace=True)

# in situations where there were only one or two moves,
# fill with the clock time
df['opponent_clock'].fillna(df['clock'], inplace=True)

# start with white
df['player_to_move'] = df['half_move'] % 2

# group by game and player; sometimes players have different clock times
# (e.g. berserk in arena)
initial_times = df.groupby(['game_link', 'player_to_move'])
# get only first row of the columns we need
initial_times = initial_times[['game_link', 'player_to_move', 'clock']].head(1)
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
df['opponent_clock_pct'] = (np.clip(df['opponent_clock'], a_min=1, a_max=None)
                            / df['opponent_initial_clock'])
df['opponent_clock_pct'] = np.clip(df['opponent_clock_pct'],
                                   a_min=None,
                                   a_max=1,
                                   )
df['opponent_sig_clock_pct'] = (np.log(df['opponent_clock_pct']
                                / (1.00001 - df['opponent_clock_pct'])))

# map to white/black colors
df['player_color_mapped'] = df['player_color'].map({'black': 1, 'white': 0})
df['white_clock'] = (df['player_color_mapped'] * df['opponent_clock']
                     + (1 - df['player_color_mapped']) * df['clock'])
df['black_clock'] = (df['player_color_mapped'] * df['clock']
                     + (1 - df['player_color_mapped']) * df['opponent_clock'])

df['white_clock_pct'] = (df['player_color_mapped'] * df['opponent_clock_pct']
                         + (1 - df['player_color_mapped']) * df['clock_pct'])
df['black_clock_pct'] = (df['player_color_mapped'] * df['clock_pct']
                         + (1 - df['player_color_mapped']) * df['opponent_clock_pct'])  # noqa

df['white_sig_clock_pct'] = (df['player_color_mapped'] * df['opponent_sig_clock_pct']  # noqa
                             + (1 - df['player_color_mapped']) * df['sig_clock_pct'])  # noqa
df['black_sig_clock_pct'] = (df['player_color_mapped'] * df['sig_clock_pct']
                             + (1 - df['player_color_mapped']) * df['opponent_sig_clock_pct'])  # noqa

df['white_elo'] = (df['player_color_mapped'] * df['opponent_elo']
                   + (1 - df['player_color_mapped']) * df['player_elo'])
df['black_elo'] = (df['player_color_mapped'] * df['player_elo']
                   + (1 - df['player_color_mapped']) * df['opponent_elo'])

df['elo_diff'] = df['white_elo'] - df['black_elo']

print('Example rows from processed dataset:')
print(df.sample(100))

max_eval = df['evaluation_binned'].max()
min_eval = df['evaluation_binned'].min()
print(f'Binned evaluation range: {min_eval} to {max_eval}')

points_per_eval = (df.groupby(['evaluation_binned'], as_index=False)
                     .agg({'result_points': 'mean'}))

df.to_csv('../../data/processed/dataset.csv', index=False)
points_per_eval.to_csv('../../data/processed/points_per_eval.csv',
                       index=False)

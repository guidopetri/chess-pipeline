#! /usr/bin/env python

import pandas as pd

pd.options.display.max_columns = 999

df = pd.read_csv('../../data/raw/dataset.csv')

df['result_points'] = df['result'].map({'1-0': 1,
                                        '1/2-1/2': 0.5,
                                        '0-1': 0})
df['evaluation_binned'] = (df['evaluation'] / 3).round(decimals=0) * 3

print('Example rows from processed dataset:')
print(df.head(100))

max_eval = df['evaluation_binned'].max()
min_eval = df['evaluation_binned'].min()
print(f'Binned evaluation range: {min_eval} to {max_eval}')

points_per_eval = (df.groupby(['evaluation_binned'], as_index=False)
                     .agg({'result_points': 'mean'}))

df.to_csv('../../data/processed/dataset.csv', index=False)
points_per_eval.to_csv('../../data/processed/points_per_eval.csv',
                       index=False)

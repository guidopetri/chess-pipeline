#! /usr/bin/env python

import pickle

import numpy as np
import pandas as pd
import statsmodels.formula.api as sm
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

df = pd.read_csv('../../data/processed/dataset.csv')

# # # # # #                    TRAIN TEST SPLIT                    # # # # # #

x_train, x_test, y_train, y_test = train_test_split(df.drop(columns=['result_points', 'result']),  # noqa
                                                    df['result_points'],
                                                    test_size=0.2,
                                                    random_state=13,
                                                    )
x_train, x_val, y_train, y_val = train_test_split(x_train,
                                                  y_train,
                                                  test_size=0.25,
                                                  random_state=13,
                                                  )

x_train.to_csv('../../data/processed/x_train.csv', index=False)
x_val.to_csv('../../data/processed/x_val.csv', index=False)
x_test.to_csv('../../data/processed/x_test.csv', index=False)
y_train.to_csv('../../data/processed/y_train.csv', index=False)
y_val.to_csv('../../data/processed/y_val.csv', index=False)
y_test.to_csv('../../data/processed/y_test.csv', index=False)

print(f'Train shape: {x_train.shape}')
print(f'Val shape: {x_val.shape}')
print(f'Test shape: {x_test.shape}')

print('Removing outliers in evaluation...')

p25 = np.percentile(x_train['evaluation'], 25)
p75 = np.percentile(x_train['evaluation'], 75)
iqr = p75 - p25

print(f'Pre-outlier removal size: {x_train.shape}')
y_train = y_train[x_train['evaluation'] > p25 - 1.5 * iqr]
y_train = y_train[x_train['evaluation'] < p75 + 1.5 * iqr]
x_train = x_train[x_train['evaluation'] > p25 - 1.5 * iqr]
x_train = x_train[x_train['evaluation'] < p75 + 1.5 * iqr]
print(f'Post-outlier removal size: {x_train.shape}')

x_train.to_csv('../../data/processed/x_train_no_outliers.csv', index=False)
y_train.to_csv('../../data/processed/y_train_no_outliers.csv', index=False)

# # # # # #                         MODELS                         # # # # # #

cols = ['elo_diff',
        'evaluation',
        'white_sig_clock_pct',
        'black_sig_clock_pct',
        'has_increment',
        ]

lr = LogisticRegression(random_state=13,
                        C=1,
                        n_jobs=-1,
                        multi_class='multinomial',
                        )

logit = sm.logit(f'result_points ~ {" + ".join(cols)}',
                 pd.concat([x_train[cols], (y_train > 0.5).astype(int)],
                           axis=1),
                 )

lr.fit(x_train[cols], y_train.astype(str))
logit = logit.fit()

with open('../../models/lr_model.pckl', 'wb') as f:
    pickle.dump(lr, f)

logit.save('../../models/logit_model.pckl')

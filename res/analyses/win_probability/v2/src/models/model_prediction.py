#! /usr/bin/env python

import pandas as pd
import pickle

with open('../../models/lr_model.pckl', 'rb') as f:
    lr = pickle.load(f)

with open('../../models/logit_model.pckl', 'rb') as f:
    logit = pickle.load(f)

x_train = pd.read_csv('../../data/processed/x_train.csv')
x_val = pd.read_csv('../../data/processed/x_val.csv')
x_test = pd.read_csv('../../data/processed/x_test.csv')
y_train = pd.read_csv('../../data/processed/y_train.csv')
y_val = pd.read_csv('../../data/processed/y_val.csv')
y_test = pd.read_csv('../../data/processed/y_test.csv')

x_train['split'] = 'train'
x_val['split'] = 'val'
x_test['split'] = 'test'

X = pd.concat([x_train, x_val, x_test],
              axis=0,
              ignore_index=True,
              )

X['y_true'] = pd.concat([y_train, y_val, y_test],
                        axis=0,
                        ignore_index=True,
                        )

cols = ['elo_diff',
        'evaluation',
        'white_sig_clock_pct',
        'black_sig_clock_pct',
        'has_increment',
        ]

print('Predicting using LR...')
print(f'Intercept: {lr.intercept_}')
print(f'Coefs: {lr.coef_}')
X['y_pred_lr'] = lr.predict_proba(X[cols]) @ [0, 0.5, 1]

print('Predicting using logit model...')
print(logit.summary())
X['y_pred_logit'] = logit.predict(X[cols])

X = X.round(6)
X['evaluation'] = X['evaluation'].round(2)

X.to_csv('../../models/predictions.csv', index=None)

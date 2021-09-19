#! /usr/bin/env python

import pandas as pd
import numpy as np
import pickle

with open('../../models/lr_model.pckl', 'rb') as f:
    lr = pickle.load(f)

with open('../../models/tumblr_model.pckl', 'rb') as f:
    tumblr = pickle.load(f)

with open('../../models/wiki_model.pckl', 'rb') as f:
    wiki = pickle.load(f)

with open('../../models/leela_model.pckl', 'rb') as f:
    leela = pickle.load(f)

x_train = pd.read_csv('../../data/processed/x_train.csv')
x_val = pd.read_csv('../../data/processed/x_val.csv')
x_test = pd.read_csv('../../data/processed/x_test.csv')
y_train = pd.read_csv('../../data/processed/y_train.csv')
y_val = pd.read_csv('../../data/processed/y_val.csv')
y_test = pd.read_csv('../../data/processed/y_test.csv')

x = pd.DataFrame(np.arange(-100, 100.01, 0.01), columns=['evaluation'])

x_train['split'] = 'train'
x_val['split'] = 'val'
x_test['split'] = 'test'
x['split'] = 'range'

X = pd.concat([x_train, x_val, x_test, x],
              axis=0,
              ignore_index=True,
              )

X['y_true'] = pd.concat([y_train,
                         y_val,
                         y_test,
                         pd.DataFrame(np.zeros(x.shape[0]),
                                      columns=['result_points']),
                         ],
                        axis=0,
                        ignore_index=True,
                        )

print('Predicting using LR...')
X['y_pred_lr'] = (lr.predict_proba(X['evaluation'].values.reshape(-1, 1))
                  @ [0, 0.5, 1])

print('Predicting using Tumblr model...')
print(', '.join([f'{k}: {v:.2f}' for k, v in tumblr.coefs().items()]))
X['y_pred_tumblr'] = tumblr.predict(X['evaluation'])

print('Predicting using wiki model...')
X['y_pred_wiki'] = wiki.predict(X['evaluation'])

print('Predicting using Leela model...')
X['y_pred_leela'] = leela.predict(X['evaluation'])

# force checkmate for black/white to be a 0/1 prediction
X = X.append([{'evaluation': 9999,
               'y_pred_lr': 1,
               'y_pred_tumblr': 1,
               'y_pred_wiki': 1,
               'y_pred_leela': 1,
               },
              {'evaluation': -9999,
               'y_pred_lr': 0,
               'y_pred_tumblr': 0,
               'y_pred_wiki': 0,
               'y_pred_leela': 0,
               },
              ],
             ignore_index=True)
X = X.round(6)
X['evaluation'] = X['evaluation'].round(2)

X.to_csv('../../models/predictions.csv', index=None)

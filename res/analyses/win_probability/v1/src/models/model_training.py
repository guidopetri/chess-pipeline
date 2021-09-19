#! /usr/bin/env python

import pandas as pd
from sklearn.linear_model import LogisticRegressionCV
from sklearn.model_selection import train_test_split
import numpy as np
import pickle
from model_definitions import tumblr_model, leela_model, wiki_model

df = pd.read_csv('../../data/processed/dataset.csv')

# # # # # #                    TRAIN TEST SPLIT                    # # # # # #

x_train, x_test, y_train, y_test = train_test_split(df['evaluation'],
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

p25 = np.percentile(x_train, 25)
p75 = np.percentile(x_train, 75)
iqr = p75 - p25

print(f'Pre-outlier removal size: {x_train.shape}')
y_train = y_train[x_train > p25 - 1.5 * iqr]
y_train = y_train[x_train < p75 + 1.5 * iqr]
x_train = x_train[x_train > p25 - 1.5 * iqr]
x_train = x_train[x_train < p75 + 1.5 * iqr]
print(f'Post-outlier removal size: {x_train.shape}')

x_train.to_csv('../../data/processed/x_train_no_outliers.csv', index=False)
y_train.to_csv('../../data/processed/y_train_no_outliers.csv', index=False)

# # # # # #                         MODELS                         # # # # # #

tumblr = tumblr_model()
wiki = wiki_model()
leela = leela_model()

lr = LogisticRegressionCV(Cs=[10 ** (x / 10) for x in range(-80, 80, 5)],
                          l1_ratios=[x / 10 for x in range(11)],
                          n_jobs=-1,
                          random_state=13,
                          penalty='elasticnet',
                          solver='saga',
                          )

lr.fit(x_train.values.reshape(-1, 1), y_train.astype(str))
tumblr.fit(x_train, y_train)
# wiki model doesn't need a fit
# neither does leela model

with open('../../models/lr_model.pckl', 'wb') as f:
    pickle.dump(lr, f)

with open('../../models/tumblr_model.pckl', 'wb') as f:
    pickle.dump(tumblr, f)

with open('../../models/wiki_model.pckl', 'wb') as f:
    pickle.dump(wiki, f)

with open('../../models/leela_model.pckl', 'wb') as f:
    pickle.dump(leela, f)

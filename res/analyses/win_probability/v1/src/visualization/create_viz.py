#! /usr/bin/env python

from itertools import product

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    roc_auc_score,
    roc_curve,
)

pd.options.display.max_columns = 999

df = pd.read_csv('../../models/predictions.csv')
points_per_eval = pd.read_csv('../../data/processed/points_per_eval.csv')

x_train = pd.read_csv('../../data/processed/x_train.csv')

plt.figure()
sns.boxplot(data=x_train.values)
plt.title('Pawn evaluations boxplot before outlier removal')
plt.savefig('../../report/plots/pre_outlier_removal_boxplot.png')

x_train = pd.read_csv('../../data/processed/x_train_no_outliers.csv')

plt.figure()
sns.boxplot(data=x_train.values)
plt.title('Pawn evaluations boxplot after outlier removal')
plt.savefig('../../report/plots/post_outlier_removal_boxplot.png')

# # # # # #                       EVALUATION                       # # # # # #

# metrics
brier = {}
auc = {}
log = {}

splits = ['train', 'val', 'test']
models = ['lr', 'tumblr', 'wiki', 'leela']

for split, model in product(splits, models):
    subset = df[df['split'] == split]
    brier[(split, model)] = brier_score_loss(subset['y_true'] == 1,
                                             subset[f'y_pred_{model}'])

    auc[(split, model)] = roc_auc_score(subset['y_true'] == 1,
                                        subset[f'y_pred_{model}'])

    log[(split, model)] = log_loss(subset['y_true'] == 1,
                                   subset[f'y_pred_{model}'])

    print(f'{model.capitalize()} performance in {split} split')
    print(f'Brier score: {brier[(split, model)]:.4f},\n'
          f'AUC: {auc[(split, model)]:.4f},\n'
          f'Log loss: {log[(split, model)]:.4f}\n')

metrics_df = pd.DataFrame.from_dict({'Brier score': brier,
                                     'AUC': auc,
                                     'Log loss': log,
                                     },
                                    orient='columns',
                                    )

metrics_df.to_markdown('../../report/plots/metrics.md')

# vs binned datapoints over the entire range
plt.figure()
plt.scatter(points_per_eval['evaluation_binned'],
            points_per_eval['result_points'],
            color='b',
            label='Binned datapoints')

for model in models:
    plt.plot(df[df['split'] == 'range']['evaluation'],
             df[df['split'] == 'range'][f'y_pred_{model}'],
             '-',
             label=f'{model.capitalize()} model')

plt.xlabel('Evaluation in pawns')
plt.ylabel('Estimated win probability')
plt.title('Models compared to binned datapoints')
plt.legend(loc='best')
plt.savefig('../../report/plots/models_vs_binned_data.png')

# calibration plots vs val
plt.figure()

for model in models:
    df[f'binned_{model}'] = df[f'y_pred_{model}'].round(decimals=1)
    calibration = (df[df['split'] == 'val']
                     .groupby(f'binned_{model}', as_index=False)  # noqa
                     .agg({'y_true': 'mean'})  # noqa
                     )  # noqa

    plt.plot(calibration[f'binned_{model}'],
             calibration['y_true'],
             label=f'{model.capitalize()}',
             )

plt.plot([0, 1], [0, 1], 'k--')

plt.xlabel('Predicted win probability')
plt.ylabel('True win probability')
plt.title('Model calibrations on the validation set')
plt.legend(loc='best')
plt.savefig('../../report/plots/val_calibrations.png')

# auc curves vs val
val_set = df[df['split'] == 'val']

plt.figure()

for model in models:
    fpr, tpr, _ = roc_curve(val_set['y_true'] == 1,
                            val_set[f'y_pred_{model}'],
                            )
    plt.plot(fpr, tpr, label=f'{model.capitalize()}')

plt.plot([0, 1], [0, 1], 'k--')

plt.xlabel('False positive rate')
plt.ylabel('True positive rate')
plt.title('ROC curves on validation set')
plt.xlim(0, 1)
plt.ylim(0, 1)
plt.legend(loc='lower right')
plt.savefig('../../report/plots/val_roc_curves.png')

(df.sample(50, random_state=13)
   .to_markdown('../../report/plots/sample_predictions.md')
   )  # noqa

# disagreeing by more than 0.1 probability
disagreements = df[(df['y_pred_lr'] - df['y_pred_leela']).abs() > 0.1]
disagreements.to_markdown('../../report/plots/model_disagreements.md')

# support across evaluation in training data
train_set = df[df['split'] == 'train']
plt.figure()
plt.hist(train_set['y_pred_lr'])
plt.title('Logistic model')
plt.xlabel('Predicted win probability')
plt.ylabel('Count')
plt.savefig('../../report/plots/lr_model_prediction_hist.png')

plt.figure()
plt.hist(train_set['evaluation'], bins=20)
plt.title('Chess engine data')
plt.xlabel('Engine evaluation in pawns')
plt.ylabel('Count')
plt.savefig('../../report/plots/train_data_support.png')

# calibration vs test
test_set = df[df['split'] == 'test'].copy()

plt.figure()

for model in models:
    calibration = (test_set.groupby(f'binned_{model}', as_index=False)
                           .agg({'y_true': 'mean'})
                           )  # noqa

    plt.plot(calibration[f'binned_{model}'],
             calibration['y_true'],
             label=f'{model.capitalize()}',
             )

plt.plot([0, 1], [0, 1], 'k--')

plt.xlabel('Predicted win probability')
plt.ylabel('True win probability')
plt.title('Model calibrations on the test set')
plt.legend(loc='best')
plt.savefig('../../report/plots/test_calibrations.png')

# add jitter for visibility
test_set['y_true'] += np.random.random(size=test_set['y_true'].shape) / 5

plt.figure()
sns.scatterplot(x='evaluation',
                y='y_true',
                hue='y_pred_lr',
                data=test_set,
                )
plt.legend(bbox_to_anchor=[1.5, 0.75])
plt.title('Out of sample datapoints, with predicted win probability as hue')
plt.savefig('../../report/plots/test_with_lr_winprob.png')

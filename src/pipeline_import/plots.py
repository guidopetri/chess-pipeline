#! /usr/bin/env python3

from seaborn import set as sns_set
from matplotlib import use
import os


def make_color_stats_plot(color_stats, fig_loc, filename):
    use('Agg')
    # set seaborn style for plots
    sns_set(style='whitegrid')

    ax = color_stats.plot(kind='bar',
                          stacked=True,
                          color='gyr',
                          ylim=(0, 1),
                          rot=0,
                          title='Win-loss ratio by color played',
                          yticks=[0.0,
                                  0.1,
                                  0.2,
                                  0.3,
                                  0.4,
                                  0.5,
                                  0.6,
                                  0.7,
                                  0.8,
                                  0.9,
                                  1.0,
                                  1.01],  # enforce two digits of precision
                          )
    ax.set_ylabel('Ratio')
    ax.set_xlabel('Category / Color')
    ax.legend().set_title('')  # remove title

    for p in ax.patches:
        # place win% in the bar itself
        ax.annotate(f'{100 * p.get_height():.2f}%',
                    xy=(0.5, 0.5),
                    xycoords=p,
                    ha='center',
                    va='center',
                    )

    # save the figure
    os.makedirs(fig_loc, exist_ok=True)
    ax.get_figure().savefig(os.path.join(fig_loc, filename),
                            bbox_inches='tight')

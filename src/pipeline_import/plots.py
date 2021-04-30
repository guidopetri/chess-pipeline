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


def make_elo_by_weekday_plot(elo, fig_loc, filename):
    use('Agg')

    sns_set(style='whitegrid')

    # plot main line with standard deviation
    ax = elo.plot(x='weekday_played',
                  y='mean',
                  yerr='std',
                  color='#0000FF',
                  title='Elo evolution by day of week',
                  style=[''],
                  legend=False,
                  capsize=4,
                  capthick=1,
                  )

    # plot min/maxes
    elo.plot(x='weekday_played',
             y=['min', 'max'],
             color='#999999',
             style=['--', '--'],
             ax=ax,
             legend=False,
             xlim=[-0.05, 6.05],
             xticks=range(0, 7),
             )

    min_last_day = elo[-1:]['min'].values
    max_last_day = elo[-1:]['max'].values
    mean_last_day = elo[-1:]['mean'].values

    # annotate the lines individually
    ax.annotate('min',
                xy=(elo.shape[0] - 0.95, min_last_day),
                color='#555555',
                )
    ax.annotate('mean + std',
                xy=(elo.shape[0] - 0.95, mean_last_day),
                color='k',
                )
    ax.annotate('max',
                xy=(elo.shape[0] - 0.95, max_last_day),
                color='#555555',
                )

    # change the tick labels
    ax.set_xticklabels(['Sunday',
                        'Monday',
                        'Tuesday',
                        'Wednesday',
                        'Thursday',
                        'Friday',
                        'Saturday',
                        ],
                       rotation=45)

    ax.set_xlabel('Weekday')
    ax.set_ylabel('Rating')

    # save the figure
    os.makedirs(fig_loc, exist_ok=True)
    ax.get_figure().savefig(os.path.join(fig_loc, filename),
                            bbox_inches='tight')

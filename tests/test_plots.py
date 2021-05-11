#! /usr/bin/env python3

from pipeline_import import plots
import pandas as pd
import os
import hashlib


def test_color_stats_plot():

    multiindex = pd.MultiIndex.from_arrays([['blitz', 'blitz', 'bullet'],
                                            ['white', 'black', 'black']],
                                           names=('time_control_category',
                                                  'player_color'))

    color_stats = pd.DataFrame([[1 / 3, 1 / 3, 1 / 3],
                                [1, 0, 0],
                                [1, 0, 0]],
                               columns=['Win', 'Draw', 'Loss'],
                               index=multiindex,
                               )

    fig_loc = '.'
    filename = 'color_stats_test.png'

    plots.make_color_stats_plot(color_stats, fig_loc, filename)

    file_loc = os.path.join(fig_loc, filename)

    with open(file_loc, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()

    true_md5 = '07fbf16c92a2544669f404e17db3e5b9'

    assert md5 == true_md5

    os.remove(file_loc)


def test_elo_by_weekday_plot():
    elo = pd.DataFrame([[0, 1666, 0, 1666.0, 1666.0],
                        [1, 1685, 7.071, 1680.0, 1690.0],
                        [2, 1685, 7.071, 1680.0, 1690.0],
                        [3, 1685, 7.071, 1680.0, 1690.0],
                        [4, 1685, 7.071, 1680.0, 1690.0],
                        [5, 1685, 7.071, 1680.0, 1690.0],
                        [6, 1662.5, 3.536, 1660.0, 1665.0]],
                       columns=['weekday_played',
                                'mean',
                                'std',
                                'min',
                                'max'],
                       )

    fig_loc = '.'
    filename = 'elo_by_weekday_test.png'

    plots.make_elo_by_weekday_plot(elo, fig_loc, filename)

    file_loc = os.path.join(fig_loc, filename)

    with open(file_loc, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()

    true_md5 = 'fec2b35e4c7d987098cdac371c3ace73'

    assert md5 == true_md5

    os.remove(file_loc)

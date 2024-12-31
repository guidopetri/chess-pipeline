#! /usr/bin/env python3

import contextlib
import hashlib
import os
import pickle
from io import BytesIO

import pandas as pd
from pipeline_import import plots
from utils.newsletter import (
    create_newsletter,
    generate_elo_by_weekday_text,
    generate_win_ratio_by_color_text,
    get_color_stats_text,
    send_newsletter,
)


def test_color_stats_plot(snapshot):

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

    assert md5 == snapshot

    os.remove(file_loc)


def test_elo_by_weekday_plot(snapshot):
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

    assert md5 == snapshot

    os.remove(file_loc)


def test_elo_by_weekday_without_games(snapshot):

    empty_elo = pd.DataFrame([],
                             columns=['weekday_played',
                                      'mean',
                                      'std',
                                      'min',
                                      'max'],
                             # enforce float dtypes,
                             # otherwise the error doesn't show up
                             dtype=float,
                             )

    fig_loc = '.'
    filename = 'elo_by_weekday_without_games.png'

    plots.make_elo_by_weekday_plot(empty_elo, fig_loc, filename)

    file_loc = os.path.join(fig_loc, filename)

    with open(file_loc, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()

    assert md5 == snapshot

    os.remove(file_loc)


def test_color_stats_text_generic():

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

    win_rate_str = get_color_stats_text(color_stats)

    true_win_rate_str = ('You had a 33.33% win rate with white in blitz and'
                         ' a 100.00% win rate with black.')

    assert win_rate_str == true_win_rate_str


def test_color_stats_text_only_one_color():

    multiindex = pd.MultiIndex.from_arrays([['blitz'],
                                            ['black']],
                                           names=('time_control_category',
                                                  'player_color'))

    color_stats = pd.DataFrame([[1 / 3, 1 / 3, 1 / 3]],
                               columns=['Win', 'Draw', 'Loss'],
                               index=multiindex,
                               )

    win_rate_str = get_color_stats_text(color_stats)

    true_win_rate_str = 'You had a 33.33% win rate with black in blitz.'

    assert win_rate_str == true_win_rate_str


def test_color_stats_text_multiple_categories():

    multiindex = pd.MultiIndex.from_arrays([['blitz', 'bullet'],
                                            ['black', 'black']],
                                           names=('time_control_category',
                                                  'player_color'))

    color_stats = pd.DataFrame([[1 / 3, 1 / 3, 1 / 3],
                                [1, 0, 0]],
                               columns=['Win', 'Draw', 'Loss'],
                               index=multiindex,
                               )

    win_rate_str = get_color_stats_text(color_stats)

    true_win_rate_str = ('You had a 33.33% win rate with black in blitz and'
                         ' a 100.00% win rate with black in bullet.')

    assert win_rate_str == true_win_rate_str


def test_generate_elo_by_weekday_text_empty():
    input_df = pd.DataFrame()
    text = generate_elo_by_weekday_text(input_df, '', '')

    assert text == '\n'


def test_generate_elo_by_weekday_text_generic(mocker, snapshot):
    # TODO: split this function off so it doesn't have to be patched
    mocker.patch('utils.newsletter.make_elo_by_weekday_plot')

    elo_df = pd.DataFrame([[100, 200], [200, 300]], columns=['min', 'max'])

    mocker.patch('utils.newsletter.get_elo_by_weekday', return_value=elo_df)

    text = generate_elo_by_weekday_text(elo_df, 'bullet', 'thibault')

    assert text == snapshot


def test_generate_win_ratio_by_color_text_empty(snapshot):
    input_df = pd.DataFrame()
    text = generate_win_ratio_by_color_text(input_df, '')

    assert text == snapshot


def test_generate_win_ratio_by_color_text_generic(mocker, snapshot):
    mocker.patch('utils.newsletter.get_color_stats')
    mocker.patch('utils.newsletter.get_color_stats_text', return_value='foo')
    # TODO: split this function off so it doesn't have to be patched
    mocker.patch('utils.newsletter.make_color_stats_plot')
    text = generate_win_ratio_by_color_text(pd.DataFrame([0]), '')

    assert text == snapshot


def test_send_newsletter(tmp_path, mocker):
    mocker.patch('utils.newsletter.get_cfg')
    mock_send = mocker.patch('utils.newsletter.SendGridAPIClient.send')
    mock_send.return_value.status_code = 202

    mocker.patch('os.path.expanduser', return_value=tmp_path / 'files')
    os.makedirs(tmp_path / 'files' / 'foobar')

    with open(tmp_path / 'files' / 'foo.bar', 'w') as f:
        f.write('dummy-input')

    text = 'foo'

    assert send_newsletter(text)
    mock_send.assert_called_once_with(text)
    assert not list((tmp_path / 'files').glob('*'))


def test_create_newsletter(tmp_path, mocker, snapshot):
    mocker.patch('utils.newsletter.get_cfg',
                 return_value={'sender': 'foo@bar.com'},
                 )

    mocker.patch('os.path.expanduser', return_value=tmp_path / 'images')
    os.mkdir(tmp_path / 'images')

    with open(tmp_path / 'images' / 'thibault.png', 'w') as f:
        f.write('dummy-input')

    class FakeLuigiInput:
        def __init__(self, value):
            self.value = value

        @contextlib.contextmanager
        def open(self, *args):
            return (x for x in [self.value])

    inputs = [FakeLuigiInput(BytesIO(pickle.dumps('foo'))),
              FakeLuigiInput(BytesIO(pickle.dumps('bar'))),
              ]

    newsletter = create_newsletter(inputs, 'thibault', 'bar@foo.com')

    assert newsletter == snapshot

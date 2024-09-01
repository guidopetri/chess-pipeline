#! /usr/bin/env python3


from collections import Counter

import pandas as pd
from feature_engineering import (
    clean_chess_df,
    explode_clocks,
    explode_materials,
    explode_moves,
    explode_positions,
)


def test_clean_chess_df(snapshot):
    pgn_input_df = pd.DataFrame(
        [['Rated bullet game',
          'https://lichess.org/KvnsPlh9',
          '2024.01.29',
          '?',
          'Nalajr',
          'thibault',
          '1-0',
          '2024.01.29',
          '09:44:48',
          '1827',
          '1794',
          '+5',
          '-14',
          'Standard',
          '120+1',
          'B30',
          'Sicilian Defense',
          'Normal',
          [0.15, 0.25, 0.24],
          [20, 20, 20],
          ['0:02:00', '0:02:00', '0:02:00'],
          False,
          False,
          False,
          {'black': 'kingside', 'white': 'kingside'},
          False,
          {True: 0, False: 0},
          {True: [], False: []},
          0,
          0,
          '',
          '',
          ['rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1',
           'rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2',
           'rnbqkbnr/pp1ppppp/8/2p5/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2',
           ],
          [Counter({'p': 8, 'P': 8, 'k': 1, 'K': 1}),
           Counter({'p': 8, 'P': 8, 'k': 1, 'K': 1}),
           Counter({'p': 8, 'P': 8, 'k': 1, 'K': 1}),
           ],
          ['e4', 'c5', 'Nf3'],
          ]],
        columns=['Event',
                 'Site',
                 'Date',
                 'Round',
                 'White',
                 'Black',
                 'Result',
                 'UTCDate',
                 'UTCTime',
                 'WhiteElo',
                 'BlackElo',
                 'WhiteRatingDiff',
                 'BlackRatingDiff',
                 'Variant',
                 'TimeControl',
                 'ECO',
                 'Opening',
                 'Termination',
                 'evaluations',
                 'eval_depths',
                 'clocks',
                 'white_berserked',
                 'black_berserked',
                 'queen_exchange',
                 'castling_sides',
                 'has_promotion',
                 'promotion_count',
                 'promotions',
                 'promotion_count_white',
                 'promotion_count_black',
                 'promotions_white',
                 'promotions_black',
                 'positions',
                 'material_by_move',
                 'moves',
                 ])
    json_input_df = pd.DataFrame(
        [['KvnsPlh9',
          True,
          'standard',
          'bullet',
          'bullet',
          1706521488926,
          1706521757537,
          'mate',
          'pool',
          'white',
          'Nalajr',
          'nalajr',
          1827,
          5,
          'thibault',
          'symbols.rainbow-flag',
          True,
          'thibault',
          1794,
          -14,
          120,
          1,
          160,
          ]],
        columns=['id',
                 'rated',
                 'variant',
                 'speed',
                 'perf',
                 'createdAt',
                 'lastMoveAt',
                 'status',
                 'source',
                 'winner',
                 'players_white_user_name',
                 'players_white_user_id',
                 'players_white_rating',
                 'players_white_ratingDiff',
                 'players_black_user_name',
                 'players_black_user_flair',
                 'players_black_user_patron',
                 'players_black_user_id',
                 'players_black_rating',
                 'players_black_ratingDiff',
                 'clock_initial',
                 'clock_increment',
                 'clock_totalTime',
                 ])
    df = clean_chess_df(pgn_input_df, json_input_df)
    assert df.reset_index(drop=True).to_json() == snapshot


def test_explode_moves(snapshot):
    input_df = pd.DataFrame([['https://fake-link.com/abc',
                              ['e4', 'c5', 'Nf3']]],
                            columns=['game_link', 'moves'])
    df = explode_moves(input_df)
    assert df.reset_index(drop=True).to_json() == snapshot


def test_explode_clocks(snapshot):
    input_df = pd.DataFrame([['https://fake-link.com/abc',
                              ['0:01:39', '0:01:45', '0:01:33']]],
                            columns=['game_link', 'clocks'])
    df = explode_clocks(input_df)
    assert df.reset_index(drop=True).to_json() == snapshot


def test_explode_positions(snapshot):
    input_df = pd.DataFrame(
        [['https://fake-link.com/abc',
          ['rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1',
           'rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2',
           'rnbqkbnr/pp1ppppp/8/2p5/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2',
           ],
          ]],
        columns=['game_link', 'positions'])
    df = explode_positions(input_df)
    assert df.reset_index(drop=True).to_json() == snapshot


def test_explode_materials(snapshot):
    input_df = pd.DataFrame([['https://fake-link.com/abc',
                              [{k: idx for idx, k in enumerate('prnbqPRNBQ')},
                               {k: idx for idx, k in enumerate('rnbqPRNBQp')},
                               ]
                              ]],
                            columns=['game_link', 'material_by_move'])
    df = explode_materials(input_df)
    assert df.reset_index(drop=True).to_json() == snapshot

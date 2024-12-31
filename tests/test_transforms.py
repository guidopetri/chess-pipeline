#! /usr/bin/env python3

import io
from subprocess import SubprocessError

import chess
import pandas as pd
import pytest
from pipeline_import import transforms, visitors
from pipeline_import.transforms import MAX_CLOUD_API_CALLS_PER_DAY


@pytest.fixture
def mock_stockfish(mocker):
    return {'location': '/stockfish', 'depth': '1'}


def test_parse_headers():

    pgn = """[Event "Rated Bullet game"]
[Site "https://lichess.org/31AU67ZY"]
[Date "2021.05.01"]
[White "Kastorcito"]
[Black "madhav116"]
[Result "0-1"]
[UTCDate "2021.05.01"]
[UTCTime "02:34:14"]
[WhiteElo "2685"]
[BlackElo "2561"]
[WhiteRatingDiff "-8"]
[BlackRatingDiff "+8"]
[WhiteTitle "GM"]
[Variant "Standard"]
[TimeControl "60+0"]
[ECO "C02"]
[Opening "French Defense"]
[Termination "Normal"]
[Annotator "lichess.org"]

1. e4 e6 2. Nf3 d5 3. e5 c5 4. d4 { C02 French Defense: Advance Variation, Nimzowitsch System } Nc6 5. Bd3 cxd4 6. O-O Qb6 7. a3 Nge7 8. b4 Ng6 9. Qe2 a6 10. h4 Be7 11. h5 Nh4 12. Nbd2 Nxf3+ 13. Nxf3 Bd7 14. h6 g6 15. Bg5 Qd8 16. Bxe7 Qxe7 17. Rab1 Qf8 18. Rfe1 Qxh6 19. a4 O-O 20. b5 axb5 21. axb5 Ne7 22. Nxd4 Rfc8 23. g4 Qg5 24. Kg2 Ra4 25. c3 Rxc3 26. Nb3 Rxg4+ 27. Kf1 Rg1# { Black wins by checkmate. } 0-1"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    visitors = []

    headers = transforms.parse_headers(game, visitors)

    moves = ['e4', 'e6', 'Nf3', 'd5', 'e5', 'c5', 'd4', 'Nc6', 'Bd3',
             'cxd4', 'O-O', 'Qb6', 'a3', 'Nge7', 'b4', 'Ng6', 'Qe2',
             'a6', 'h4', 'Be7', 'h5', 'Nh4', 'Nbd2', 'Nxf3+', 'Nxf3',
             'Bd7', 'h6', 'g6', 'Bg5', 'Qd8', 'Bxe7', 'Qxe7', 'Rab1',
             'Qf8', 'Rfe1', 'Qxh6', 'a4', 'O-O', 'b5', 'axb5', 'axb5',
             'Ne7', 'Nxd4', 'Rfc8', 'g4', 'Qg5', 'Kg2', 'Ra4', 'c3',
             'Rxc3', 'Nb3', 'Rxg4+', 'Kf1', 'Rg1#',
             ]

    true_headers = {'Event': 'Rated Bullet game',
                    'Round': '?',
                    'Site': 'https://lichess.org/31AU67ZY',
                    'Date': '2021.05.01',
                    'White': 'Kastorcito',
                    'Black': 'madhav116',
                    'Result': '0-1',
                    'UTCDate': '2021.05.01',
                    'UTCTime': '02:34:14',
                    'WhiteElo': '2685',
                    'BlackElo': '2561',
                    'WhiteRatingDiff': '-8',
                    'BlackRatingDiff': '+8',
                    'WhiteTitle': 'GM',
                    'Variant': 'Standard',
                    'TimeControl': '60+0',
                    'ECO': 'C02',
                    'Opening': 'French Defense',
                    'Termination': 'Normal',
                    'Annotator': 'lichess.org',
                    'moves': moves,
                    }

    assert headers == true_headers


def test_parse_headers_position_variant():

    pgn = """[Variant "From Position"]

1. e4 e6 0-1"""

    game = chess.pgn.read_game(io.StringIO(pgn))

    visitors = []

    headers = transforms.parse_headers(game, visitors)

    assert headers['Variant'] == 'Standard'


def test_parse_headers_no_variant_header():

    pgn = """1. e4 e6 0-1"""

    game = chess.pgn.read_game(io.StringIO(pgn))

    visitors = []

    headers = transforms.parse_headers(game, visitors)

    assert headers['Variant'] == 'Standard'


def test_parse_headers_visitor():

    pgn = """1. d4 e5 2. dxe5"""

    game = chess.pgn.read_game(io.StringIO(pgn))

    visitor = [visitors.MaterialVisitor]

    headers = transforms.parse_headers(game, visitor)

    materials = [{'p': 8, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 {'p': 8, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 {'p': 8, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 {'p': 7, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 ]

    assert headers['material_by_move'] == materials


def test_fix_provisional_columns_missing_neither():

    data = pd.DataFrame([[False, None], [None, True]],
                        columns=['players_white_provisional',
                                 'players_black_provisional',
                                 ],
                        dtype=bool)

    clean = pd.DataFrame([[False, False], [False, True]],
                         columns=['players_white_provisional',
                                  'players_black_provisional',
                                  ])

    parsed = transforms.fix_provisional_columns(data)

    pd.testing.assert_frame_equal(parsed, clean, check_like=True)


def test_fix_provisional_columns_missing_black():

    data = pd.DataFrame([[0, 0, False], [0, 0, False]],
                        columns=['white_rating',
                                 'black_rating',
                                 'players_white_provisional',
                                 ])

    clean = pd.DataFrame([[0, 0, False, False], [0, 0, False, False]],
                         columns=['white_rating',
                                  'black_rating',
                                  'players_black_provisional',
                                  'players_white_provisional',
                                  ])

    parsed = transforms.fix_provisional_columns(data)

    pd.testing.assert_frame_equal(parsed, clean, check_like=True)


def test_fix_provisional_columns_missing_white():

    data = pd.DataFrame([[0, 0, False], [0, 0, False]],
                        columns=['white_rating',
                                 'black_rating',
                                 'players_black_provisional',
                                 ])

    clean = pd.DataFrame([[0, 0, False, False], [0, 0, False, False]],
                         columns=['white_rating',
                                  'black_rating',
                                  'players_black_provisional',
                                  'players_white_provisional',
                                  ])

    parsed = transforms.fix_provisional_columns(data)

    pd.testing.assert_frame_equal(parsed, clean, check_like=True)


def test_fix_provisional_columns_missing_both():

    data = pd.DataFrame([[0, 0], [0, 0]],
                        columns=['white_rating',
                                 'black_rating',
                                 ])

    clean = pd.DataFrame([[0, 0, False, False], [0, 0, False, False]],
                         columns=['white_rating',
                                  'black_rating',
                                  'players_black_provisional',
                                  'players_white_provisional',
                                  ])

    parsed = transforms.fix_provisional_columns(data)

    pd.testing.assert_frame_equal(parsed, clean, check_like=True)


def test_fix_provisional_columns_full_data():

    data = pd.DataFrame([[False, True], [True, True]],
                        columns=['players_white_provisional',
                                 'players_black_provisional',
                                 ])

    clean = pd.DataFrame([[False, True], [True, True]],
                         columns=['players_white_provisional',
                                  'players_black_provisional',
                                  ])

    parsed = transforms.fix_provisional_columns(data)

    pd.testing.assert_frame_equal(parsed, clean, check_like=True)


def test_get_sf_evaluation_cloud(mocker):
    mock_parsed_resp = {'pvs': [{'cp': -30}]}

    mocker.patch('lichess.api.cloud_eval', return_value=mock_parsed_resp)

    fen = 'r1bqkb1r/pp1ppppp/2n2n2/2p5/8/1P3NP1/PBPPPP1P/RN1QKB1R b KQkq - 0 1'

    # loc/depth don't matter
    rating = transforms.get_sf_evaluation(fen,
                                          '',
                                          1,
                                          valkey_client=mocker.MagicMock(),
                                          )

    assert rating == -0.3


@pytest.fixture
def mock_valkey_client():
    class MockValkey:
        def __init__(self):
            self.counter = 0

        def get(self, *args, **kwargs):
            return '0'

        def incr(self, *args, **kwargs):
            self.counter += 1

        def expireat(self, *args, **kwargs):
            pass
    return MockValkey()


def test_get_sf_evaluation_tracks_api_calls(mocker, mock_valkey_client):
    mock_parsed_resp = {'pvs': [{'cp': -30}]}

    mocker.patch('lichess.api.cloud_eval', return_value=mock_parsed_resp)

    # loc/depth don't matter
    transforms.get_sf_evaluation('',
                                 '',
                                 1,
                                 valkey_client=mock_valkey_client,
                                 )

    assert mock_valkey_client.counter == 1


def test_get_sf_evaluation_doesnt_exceed_api_calls(mocker, mock_valkey_client):
    mock_sf = mocker.patch('stockfish.Stockfish')
    mocker.patch('re.search', return_value=None)

    mock_valkey_client.counter = MAX_CLOUD_API_CALLS_PER_DAY + 1

    with pytest.raises(SubprocessError):
        transforms.get_sf_evaluation('',
                                     '',
                                     1,
                                     valkey_client=mock_valkey_client,
                                     )

    mock_sf.assert_called_once()


def test_get_sf_evaluation_cloud_mate_in_x(mocker):
    mock_parsed_resp = {'pvs': [{'mate': 1}]}

    mocker.patch('lichess.api.cloud_eval', return_value=mock_parsed_resp)

    # scholar's mate
    fen = 'r1bqkbnr/ppp2ppp/2np4/4p3/2B1P3/5Q2/PPPP1PPP/RNB1K1NR w KQkq - 2 4'

    # loc/depth don't matter
    rating = transforms.get_sf_evaluation(fen,
                                          '',
                                          1,
                                          valkey_client=mocker.MagicMock(),
                                          )

    assert rating == 9999


def test_get_sf_evaluation_cloud_error(mocker):
    mocker.patch('lichess.api.cloud_eval', return_value={'pvs': ['foobar']})
    with pytest.raises(KeyError):
        transforms.get_sf_evaluation('fake fen',
                                     '',
                                     1,
                                     valkey_client=mocker.MagicMock(),
                                     )


def test_get_sf_evaluation_local_returns_error(mocker, mocked_cloud_eval):
    mocker.patch('stockfish.Stockfish')
    mocker.patch('re.search', return_value=None)

    with pytest.raises(SubprocessError):
        transforms.get_sf_evaluation('', '', 1)


def test_get_sf_evaluation_shallow(mock_stockfish, mocked_cloud_eval):

    fen = 'r1bq1rk1/1pp3b1/3p2np/nP2P1p1/4Pp2/PN3NP1/1B3PBP/R2Q1RK1 b - - 2 0'
    stockfish_loc = mock_stockfish['location']

    depth = 10

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    if '10' in stockfish_loc:
        assert rating == -0.52
    elif '11' in stockfish_loc:
        assert rating == -0.89
    elif '12' in stockfish_loc:
        assert rating == -0.89
    elif '13' in stockfish_loc:
        assert rating == -0.89


def test_get_sf_evaluation_deep(mock_stockfish, mocked_cloud_eval):

    fen = 'r1bq1rk1/1pp3b1/3p2np/nP2P1p1/4Pp2/PN3NP1/1B3PBP/R2Q1RK1 b - - 2 0'
    stockfish_loc = mock_stockfish['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    if '10' in stockfish_loc:
        assert rating == -0.89
    elif '11' in stockfish_loc:
        assert rating == -0.89
    elif '12' in stockfish_loc:
        assert rating == -0.89
    elif '13' in stockfish_loc:
        assert rating == -0.89


def test_get_sf_evaluation_checkmate_black(mock_stockfish, mocked_cloud_eval):

    fen = '8/5q1k/7p/4Q2r/P3P3/4R1P1/7p/3R1r1K w - - 3 0'
    stockfish_loc = mock_stockfish['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == -9999


def test_get_sf_evaluation_checkmate_white(mock_stockfish, mocked_cloud_eval):

    fen = '5rk1/4Q1b1/8/pp6/8/7N/1P2R1PK/8 w - - 1 0'
    stockfish_loc = mock_stockfish['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == 9999


def test_get_sf_evaluation_in_stalemate(mock_stockfish, mocked_cloud_eval):

    fen = '3Q4/8/8/8/8/3QK2P/8/4k3 b - - 0 56'
    stockfish_loc = mock_stockfish['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == 0


def test_get_sf_evaluation_in_checkmate(mock_stockfish, mocked_cloud_eval):

    fen = '4Rb1k/7Q/8/1p4N1/p7/8/1P4PK/8 b - - 4 0'
    stockfish_loc = mock_stockfish['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == 9999


def test_get_sf_evaluation_double_checkmate(mock_stockfish, mocked_cloud_eval):

    fen = '6k1/4pppp/6r1/3b4/4r3/8/1Q5P/1R5K w - - 0 0'
    stockfish_loc = mock_stockfish['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == 9999

    fen = '6k1/4pppp/6r1/3b4/4r3/8/1Q5P/1R5K b - - 0 0'

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == -9999


def test_convert_clock_to_seconds():
    data = pd.Series(['0:00:03', '0:01:00', '0:10:39', None, 'NotATimedelta'])

    clean = pd.Series([3, 60, 639, -1, -1], dtype=int)

    parsed = transforms.convert_clock_to_seconds(data)

    pd.testing.assert_series_equal(parsed, clean)


def test_get_clean_fens():
    fen = 'r3rnk1/ppq2ppn/2pb4/3pN1p1/3P1P1B/2PB4/PPQ3PP/R3R1K1 w - - 0 19'
    clean = 'r3rnk1/ppq2ppn/2pb4/3pN1p1/3P1P1B/2PB4/PPQ3PP/R3R1K1 w - - 0'

    fen = pd.Series([fen])
    clean = pd.Series([clean])
    assert (transforms.get_clean_fens(fen) == clean).all()

    fen = 'r1bqkbnr/pp3ppp/2n1p3/2ppP3/3P4/3B1N2/PPP2PPP/RNBQK2R b KQkq - 2 5'
    clean = 'r1bqkbnr/pp3ppp/2n1p3/2ppP3/3P4/3B1N2/PPP2PPP/RNBQK2R b KQkq - 2'

    fen = pd.Series([fen])
    clean = pd.Series([clean])
    assert (transforms.get_clean_fens(fen) == clean).all()


def test_transform_game_data(tmp_path):
    player = 'thibault'

    # fake game, this is dummy data anyway
    headers = {'event_type': 'Rated Bullet game',
               'round': '?',
               'game_link': 'https://lichess.org/31AU67ZY',
               'date_played': '2020.05.01',
               'white': 'Kastorcito',
               'black': 'thibault',
               'result': '0-1',
               'utc_date_played': '2021.05.01',
               'time_played': '02:34:14',
               'white_elo': '2685',
               'black_elo': '2561',
               'white_rating_diff': '-8',
               'black_rating_diff': '+8',
               'chess_variant': 'Standard',
               'time_control': '60+0',
               'opening_played': 'C02',
               'lichess_opening': 'French Defense',
               'termination': 'Normal',
               'players_black_provisional': False,
               'players_white_provisional': False,
               'queen_exchange': True,
               'castling_sides': [{'black': 'kingside', 'white': 'queenside'}],
               'speed': 'bullet',
               }

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers = {'event_type': 'Rated Bullet game',
                    'round': '?',
                    'game_link': 'https://lichess.org/31AU67ZY',
                    'white': 'Kastorcito',
                    'black': 'thibault',
                    'result': '0-1',
                    'utc_date_played': pd.to_datetime('2021.05.01'),
                    'time_played': '02:34:14',
                    'white_elo': '2685',
                    'black_elo': '2561',
                    'white_rating_diff': '-8',
                    'black_rating_diff': '+8',
                    'chess_variant': 'Standard',
                    'time_control': '60+0',
                    'opening_played': 'C02',
                    'lichess_opening': 'French Defense',
                    'termination': 'Normal',
                    'players_black_provisional': False,
                    'players_white_provisional': False,
                    'castling_sides': [{'black': 'kingside',
                                        'white': 'queenside'}],
                    'queen_exchange': 'Queen exchange',
                    'player_castling_side': 'kingside',
                    'opponent_castling_side': 'queenside',
                    'player': player,
                    'opponent': 'Kastorcito',
                    'player_color': 'black',
                    'opponent_color': 'white',
                    'player_elo': 2561,
                    'opponent_elo': 2685,
                    'player_rating_diff': 8,
                    'opponent_rating_diff': -8,
                    'player_result': 'Win',
                    'opponent_result': 'Loss',
                    'time_control_category': 'bullet',
                    'datetime_played': pd.to_datetime('2021-05-01 02:34:14'),
                    'starting_time': 60,
                    'increment': 0,
                    'in_arena': 'Not in arena',
                    'rated_casual': 'Rated',
                    'date_played': pd.to_datetime('2020-05-01'),
                    }

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test rating diff insertion
    del headers['black_rating_diff']
    del headers['white_rating_diff']

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['white_rating_diff'] = 0
    true_headers['player_rating_diff'] = 0
    true_headers['black_rating_diff'] = 0
    true_headers['opponent_rating_diff'] = 0

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test draw
    headers['result'] = '1/2-1/2'

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['result'] = '1/2-1/2'
    true_headers['player_result'] = 'Draw'
    true_headers['opponent_result'] = 'Draw'

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test arena
    headers['event_type'] = 'Rated Bullet Arena'

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['event_type'] = 'Rated Bullet Arena'
    true_headers['in_arena'] = 'In arena'

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test rated/casual
    headers['event_type'] = 'Casual Bullet Arena'

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['event_type'] = 'Casual Bullet Arena'
    true_headers['rated_casual'] = 'Casual'

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test queen exchange
    headers['queen_exchange'] = False

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['queen_exchange'] = 'No queen exchange'

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test castling side
    headers['castling_sides'] = [{'black': 'queenside', 'white': None}]
    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['player_castling_side'] = 'queenside'
    true_headers['opponent_castling_side'] = 'No castling'
    true_headers['castling_sides'] = [{'black': 'queenside', 'white': None}]

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)

    #####################################

    # test anonymous player ratings

    headers['white_elo'] = '?'

    df = pd.DataFrame(headers)
    df.to_parquet(tmp_path / 'cleaned_df.parquet')

    transforms.transform_game_data(player='thibault',
                                   perf_type='',
                                   data_date='',
                                   local_stockfish=True,
                                   io_dir=tmp_path,
                                   )
    parsed = pd.read_parquet(tmp_path / 'game_infos.parquet')

    true_headers['white_elo'] = '?'
    true_headers['opponent_elo'] = 1500
    true_headers['opponent_rating_diff'] = 0

    true = pd.DataFrame(true_headers)

    pd.testing.assert_frame_equal(parsed, true, check_like=True)


def test_get_color_stats():

    data = pd.DataFrame([[13, 'blitz', 'white', 'Win'],
                         [14, 'blitz', 'white', 'Loss'],
                         [15, 'blitz', 'white', 'Draw'],
                         [16, 'blitz', 'black', 'Win'],
                         [17, 'blitz', 'black', 'Win'],
                         [18, 'bullet', 'black', 'Win'],
                         ],
                        columns=['id',
                                 'time_control_category',
                                 'player_color',
                                 'player_result',
                                 ])

    parsed = transforms.get_color_stats(data)

    multiindex = pd.MultiIndex.from_arrays([['blitz', 'blitz', 'bullet'],
                                            ['white', 'black', 'black']],
                                           names=('time_control_category',
                                                  'player_color'))

    true = pd.DataFrame([[1 / 3, 1 / 3, 1 / 3],
                         [1, 0, 0],
                         [1, 0, 0]],
                        columns=pd.Index(['Win', 'Draw', 'Loss'],
                                         name='player_result'),
                        index=multiindex,
                        )

    pd.testing.assert_frame_equal(parsed, true, check_like=True)


def test_get_color_stats_missing_category():

    data = pd.DataFrame([[13, 'blitz', 'white', 'Win'],
                         [14, 'blitz', 'white', 'Loss'],
                         [15, 'blitz', 'black', 'Win'],
                         [16, 'blitz', 'black', 'Win'],
                         [17, 'bullet', 'black', 'Win'],
                         ],
                        columns=['id',
                                 'time_control_category',
                                 'player_color',
                                 'player_result',
                                 ])

    parsed = transforms.get_color_stats(data)

    multiindex = pd.MultiIndex.from_arrays([['blitz', 'blitz', 'bullet'],
                                            ['white', 'black', 'black']],
                                           names=('time_control_category',
                                                  'player_color'))

    true = pd.DataFrame([[0.5, 0, 0.5],
                         [1, 0, 0],
                         [1, 0, 0]],
                        columns=pd.Index(['Win', 'Draw', 'Loss'],
                                         name='player_result'),
                        index=multiindex,
                        )

    pd.testing.assert_frame_equal(parsed, true, check_like=True)


def test_get_elo_by_weekday():

    # disable SettingWithCopy warning
    pd.options.mode.chained_assignment = None

    # make sure we get sundays + mondays
    data = pd.DataFrame([[13, 'blitz', '2021-05-10', 1680],
                         [14, 'blitz', '2021-05-10', 1690],
                         [15, 'blitz', '2021-05-09', 1666],
                         [16, 'blitz', '2021-05-08', 1660],
                         [17, 'blitz', '2021-05-08', 1665],
                         [18, 'bullet', '2021-05-10', 2800],
                         ],
                        columns=['id',
                                 'time_control_category',
                                 'datetime_played',
                                 'player_elo',
                                 ])
    data['datetime_played'] = pd.to_datetime(data['datetime_played'])

    parsed = transforms.get_elo_by_weekday(data, category='blitz')

    true = pd.DataFrame([[0, 1666, 0, 1666.0, 1666.0],
                         [1, 1685, 7.071, 1680.0, 1690.0],
                         [6, 1662.5, 3.536, 1660.0, 1665.0]],
                        columns=['weekday_played',
                                 'mean',
                                 'std',
                                 'min',
                                 'max'],
                        )
    true['weekday_played'] = true['weekday_played'].astype('int32')

    pd.testing.assert_frame_equal(parsed,
                                  true,
                                  check_like=True,
                                  atol=1e-2,
                                  )

    parsed = transforms.get_elo_by_weekday(data, category='bullet')

    true = pd.DataFrame([[1, 2800.0, 0.0, 2800.0, 2800.0]],
                        columns=['weekday_played',
                                 'mean',
                                 'std',
                                 'min',
                                 'max'],
                        )
    true['weekday_played'] = true['weekday_played'].astype('int32')

    pd.testing.assert_frame_equal(parsed,
                                  true,
                                  check_like=True,
                                  atol=1e-2,
                                  )

    # re-enable SettingWithCopy warning
    pd.options.mode.chained_assignment = 'warn'


def test_get_weekly_data(mocker):
    # TODO: what is this test testing, exactly?
    # maybe make it work outside of docker compose
    cfg = {'host': 'chess_pipeline_postgres',
           'port': 5432,
           'database': 'chess_db',
           'read_user': 'read_user',
           'read_password': 'read_password',
           }
    mocker.patch('pipeline_import.transforms.get_cfg', return_value=cfg)

    player = 'thibault'

    data = transforms.get_weekly_data(cfg, player)

    cols = ['event_type',
            'result',
            'round',
            'game_link',
            'termination',
            'chess_variant',
            'black_elo_tentative',
            'white_elo_tentative',
            'player',
            'opponent',
            'player_color',
            'opponent_color',
            'player_rating_diff',
            'opponent_rating_diff',
            'player_result',
            'opponent_result',
            'time_control_category',
            'datetime_played',
            'starting_time',
            'increment',
            'in_arena',
            'rated_casual',
            'player_elo',
            'opponent_elo',
            'queen_exchange',
            'player_castling_side',
            'opponent_castling_side',
            'lichess_opening',
            'opening_played',
            'has_promotion',
            'promotion_count_white',
            'promotion_count_black',
            'promotions_white',
            'promotions_black',
            'black_berserked',
            'white_berserked',
            ]

    assert all([col in data.columns for col in cols])

    timedeltas = pd.Timestamp.now() - data['datetime_played']

    assert (timedeltas <= pd.Timedelta('7 days')).all()

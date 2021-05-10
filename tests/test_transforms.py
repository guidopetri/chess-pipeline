#! /usr/bin/env python3

from pipeline_import import transforms
from configparser import ConfigParser
import pandas as pd
import io
import chess


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

    visitor_stats = {}
    headers = transforms.parse_headers(game, visitors, visitor_stats)

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

    visitor_stats = {}
    headers = transforms.parse_headers(game, visitors, visitor_stats)

    assert headers['Variant'] == 'Standard'


def test_fix_provisional_columns_missing_neither():

    data = pd.DataFrame([[False, None], [None, True]],
                        columns=['players_white_provisional',
                                 'players_black_provisional',
                                 ])

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


def test_get_sf_evaluation_shallow():

    fen = 'r1bq1rk1/1pp3b1/3p2np/nP2P1p1/4Pp2/PN3NP1/1B3PBP/R2Q1RK1 b - - 2 0'

    cfg = ConfigParser()
    cfg.read('luigi.cfg')
    stockfish_loc = cfg['stockfish_cfg']['location']

    depth = 10

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == -0.52


def test_get_sf_evaluation_deep():

    fen = 'r1bq1rk1/1pp3b1/3p2np/nP2P1p1/4Pp2/PN3NP1/1B3PBP/R2Q1RK1 b - - 2 0'

    cfg = ConfigParser()
    cfg.read('luigi.cfg')
    stockfish_loc = cfg['stockfish_cfg']['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == -0.89


def test_get_sf_evaluation_checkmate_black():

    fen = '8/5q1k/7p/4Q2r/P3P3/4R1P1/7p/3R1r1K w - - 3 0'

    cfg = ConfigParser()
    cfg.read('luigi.cfg')
    stockfish_loc = cfg['stockfish_cfg']['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == -9999


def test_get_sf_evaluation_checkmate_white():

    fen = '5rk1/4Q1b1/8/pp6/8/7N/1P2R1PK/8 w - - 1 0'

    cfg = ConfigParser()
    cfg.read('luigi.cfg')
    stockfish_loc = cfg['stockfish_cfg']['location']

    depth = 20

    rating = transforms.get_sf_evaluation(fen, stockfish_loc, depth)

    assert rating == 9999


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


def test_transform_game_data():
    assert False


def test_get_color_stats():
    assert False


def test_get_elo_by_weekday():
    assert False


def test_get_weekly_data():
    assert False

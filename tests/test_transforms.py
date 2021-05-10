#! /usr/bin/env python3

from pipeline_import import transforms
import pandas as pd
import io
import chess


def test_parse_headers():
    from pipeline_import.visitors import EvalsVisitor, ClocksVisitor
    from pipeline_import.visitors import QueenExchangeVisitor, CastlingVisitor
    from pipeline_import.visitors import PromotionsVisitor, PositionsVisitor
    from pipeline_import.visitors import MaterialVisitor

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
[Opening "French Defense: Advance Variation, Nimzowitsch System"]
[Termination "Normal"]
[Annotator "lichess.org"]

1. e4 e6 2. Nf3 d5 3. e5 c5 4. d4 { C02 French Defense: Advance Variation, Nimzowitsch System } Nc6 5. Bd3 cxd4 6. O-O Qb6 7. a3 Nge7 8. b4 Ng6 9. Qe2 a6 10. h4 Be7 11. h5 Nh4 12. Nbd2 Nxf3+ 13. Nxf3 Bd7 14. h6 g6 15. Bg5 Qd8 16. Bxe7 Qxe7 17. Rab1 Qf8 18. Rfe1 Qxh6 19. a4 O-O 20. b5 axb5 21. axb5 Ne7 22. Nxd4 Rfc8 23. g4 Qg5 24. Kg2 Ra4 25. c3 Rxc3 26. Nb3 Rxg4+ 27. Kf1 Rg1# { Black wins by checkmate. } 0-1"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    visitors = [EvalsVisitor,
                ClocksVisitor,
                QueenExchangeVisitor,
                CastlingVisitor,
                PromotionsVisitor,
                PositionsVisitor,
                MaterialVisitor,
                ]

    visitor_stats = {'clocks': 'clocks',
                     'evaluations': 'evals',
                     'eval_depths': 'eval_depths',
                     'queen_exchange': 'queen_exchange',
                     'castling_sides': 'castling',
                     'has_promotion': 'has_promotion',
                     'promotion_count_white': 'promotion_count_white',
                     'promotion_count_black': 'promotion_count_black',
                     'promotions_white': 'promotions_white',
                     'promotions_black': 'promotions_black',
                     'positions': 'positions',
                     'black_berserked': 'black_berserked',
                     'white_berserked': 'white_berserked',
                     'material_by_move': 'material_by_move',
                     }
    headers = transforms.parse_headers(game, visitors, visitor_stats)

    assert headers is False


def test_fix_provisional_columns():
    assert False


def test_get_sf_evaluation():
    assert False


def test_convert_clock_to_seconds():
    assert False


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

#! /usr/bin/env python3

import io

import chess
import pytest
from pipeline_import import visitors


def test_evals_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 { [%eval 0.05] } 1... c5 { [%eval 0.32] } 2. f4 { [%eval #3] } 2... d6 { [%eval #-3] } 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.EvalsVisitor(game))

    assert game.headers['evaluations'] == [0.05, 0.32, 9999, -9999]
    assert all([x == 20 for x in game.headers['eval_depths']])


def test_clocks_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 { [%clk 0:01:00] } 1... c5 { [%clk 0:01:00] } 2. f4 { [%clk 0:00:59] } 2... d6 { [%clk 0:01:00] } 3. e5 { [%eval 0.3] } 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.ClocksVisitor(game))

    assert game.headers['clocks'] == ['0:01:00',
                                      '0:01:00',
                                      '0:00:59',
                                      '0:01:00',
                                      '',
                                      ]


def test_queen_exchange_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 c5 2. f4 d6 3. Nf3 Nf6 4. d3 g6 5. c3 Bg7 6. e5 dxe5 7. fxe5 Nd5 8. d4 cxd4 9. cxd4 O-O 10. Nc3 Nc6 11. Nxd5 Qxd5 12. Be3 Bg4 13. Be2 Bxf3 14. Bxf3 Qa5+ 15. Bd2 Qb5 16. Bc3 Rad8 17. Be2 Qb6 18. d5 Nxe5 19. Bxe5 Bxe5 20. Qd3 Qxb2 21. O-O Qd4+ 22. Kh1 Qxd3 23. Bxd3 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.QueenExchangeVisitor(game))

    assert game.headers['queen_exchange']


def test_castling_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 c5 2. f4 d6 3. Nf3 Nf6 4. d3 g6 5. c3 Bg7 6. e5 dxe5 7. fxe5 Nd5 8. d4 cxd4 9. cxd4 O-O 10. Nc3 Nc6 11. Nxd5 Qxd5 12. Be3 Bg4 13. Be2 Bxf3 14. Bxf3 Qa5+ 15. Bd2 Qb5 16. Bc3 Rad8 17. Be2 Qb6 18. d5 Nxe5 19. Bxe5 Bxe5 20. Qd3 Qxb2 21. O-O Qd4+ 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.CastlingVisitor(game))

    assert game.headers['castling_sides']['white'] == 'kingside'
    assert game.headers['castling_sides']['black'] == 'kingside'

    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 c5 2. f4 d6 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.CastlingVisitor(game))

    assert game.headers['castling_sides']['white'] is None
    assert game.headers['castling_sides']['black'] is None

    pgn = """[Site "https://lichess.org/oUMAQzs2"]

1. d4 Nf6 2. c4 c5 3. d5 g6 4. Nc3 d6 5. Bg5 Bg7 6. Qd2 O-O 7. Bh6 Qb6 8. Bxg7 Kxg7 9. h4 h5 10. f3 e6 11. g4 exd5 12. cxd5 Nbd7 13. e3 Ne5 14. Be2 Qa5 15. f4 Nexg4 16. Bxg4 Nxg4 17. O-O-O Bf5 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.CastlingVisitor(game))

    assert game.headers['castling_sides']['white'] == 'queenside'
    assert game.headers['castling_sides']['black'] == 'kingside'


@pytest.mark.skip
def test_positions_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 c5 2. f4 d6 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.PositionsVisitor(game))

    true = ['rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1',
            'rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1',
            'rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2',
            'rnbqkbnr/pp1ppppp/8/2p5/4PP2/8/PPPP2PP/RNBQKBNR b KQkq - 0 2',
            'rnbqkbnr/pp2pppp/3p4/2p5/4PP2/8/PPPP2PP/RNBQKBNR w KQkq - 0 3',
            ]

    assert game.headers['positions'] == true


def test_promotions_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 c5 2. f4 d6 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.PromotionsVisitor(game))

    assert not game.headers['has_promotion']
    assert game.headers['promotion_count'] == {chess.WHITE: 0, chess.BLACK: 0}
    assert game.headers['promotions'] == {chess.WHITE: [], chess.BLACK: []}
    assert game.headers['promotion_count_white'] == 0
    assert game.headers['promotion_count_black'] == 0
    assert game.headers['promotions_white'] == ''
    assert game.headers['promotions_black'] == ''

    pgn = """[Site "https://lichess.org/vepGKt97"]

1. d4 d5 2. Bf4 Bf5 3. c4 e6 4. Nc3 c6 5. Qb3 Qb6 6. Qxb6 axb6 7. Nf3 Nd7 8. e3 Ngf6 9. cxd5 exd5 10. h3 Be7 11. g4 Bg6 12. g5 Ne4 13. Nxe4 Bxe4 14. Bg2 O-O 15. h4 Ra4 16. O-O Rfa8 17. a3 b5 18. Ne5 Nxe5 19. Bxe5 Bxg2 20. Kxg2 b4 21. Bc7 bxa3 22. bxa3 Rxa3 23. Rxa3 Rxa3 24. Rb1 b5 25. Bb6 Ra6 26. Bc5 Bxc5 27. dxc5 Kf8 28. Kf3 Ra4 29. Kg3 Ke7 30. f4 Ke6 31. Kf3 Kf5 32. Rd1 Rc4 33. h5 Rxc5 34. Rd4 Rc4 35. e4+ dxe4+ 36. Ke3 Rxd4 37. Kxd4 b4 38. Kc5 e3 39. Kxb4 e2 40. Kc5 e1=Q 41. Kxc6 Qe4+ 42. Kd7 Qxf4 43. h6 gxh6 44. Ke7 Qxg5+ 45. Kf8 h5 46. Kxf7 h4 47. Kf8 h3 48. Kf7 h2 49. Kf8 h1=Q 50. Ke8 Qb7 51. Kf8 Qgg7+ 52. Ke8 Qg8# 0-1"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.PromotionsVisitor(game))

    assert game.headers['has_promotion']
    assert game.headers['promotion_count'] == {chess.WHITE: 0, chess.BLACK: 2}
    assert game.headers['promotions'] == {chess.WHITE: [],
                                          chess.BLACK: ['q', 'q']}
    assert game.headers['promotion_count_white'] == 0
    assert game.headers['promotion_count_black'] == 2
    assert game.headers['promotions_white'] == ''
    assert game.headers['promotions_black'] == 'qq'


def test_materials_visitor():

    pgn = """1. d4 e5 2. dxe5"""

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.MaterialVisitor(game))

    materials = [{'p': 8, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 {'p': 8, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 {'p': 8, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 {'p': 7, 'b': 2, 'r': 2, 'q': 1, 'k': 1, 'n': 2,
                  'P': 8, 'B': 2, 'R': 2, 'Q': 1, 'K': 1, 'N': 2},
                 ]

    assert game.headers['material_by_move'] == materials

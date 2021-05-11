#! /usr/bin/env python3

from pipeline_import import visitors
import io
import chess


def test_evals_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 { [%eval 0.05] } 1... c5 { [%eval 0.32] } 2. f4 { [%eval #3] } 2... d6 { [%eval #-3] } 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.EvalsVisitor(game))

    assert game.evals == [0.05, 0.32, 9999, -9999]
    assert all([x == 20 for x in game.eval_depths])


def test_clocks_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 { [%clk 0:01:00] } 1... c5 { [%clk 0:01:00] } 2. f4 { [%clk 0:00:59] } 2... d6 { [%clk 0:01:00] } 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.ClocksVisitor(game))

    assert game.clocks == ['0:01:00', '0:01:00', '0:00:59', '0:01:00']


def test_queen_exchange_visitor():
    pgn = """[Site "https://lichess.org/TTYLmSUX"]

1. e4 c5 2. f4 d6 3. Nf3 Nf6 4. d3 g6 5. c3 Bg7 6. e5 dxe5 7. fxe5 Nd5 8. d4 cxd4 9. cxd4 O-O 10. Nc3 Nc6 11. Nxd5 Qxd5 12. Be3 Bg4 13. Be2 Bxf3 14. Bxf3 Qa5+ 15. Bd2 Qb5 16. Bc3 Rad8 17. Be2 Qb6 18. d5 Nxe5 19. Bxe5 Bxe5 20. Qd3 Qxb2 21. O-O Qd4+ 22. Kh1 Qxd3 23. Bxd3 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.QueenExchangeVisitor(game))

    assert game.queen_exchange


def test_castling_visitor():
    assert False


def test_positions_visitor():
    assert False


def test_promotions_visitor():
    assert False


def test_materials_visitor():
    assert False

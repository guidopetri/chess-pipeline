#! /usr/bin/env python3

from pipeline_import import visitors
import io
import chess


def test_evals_visitor():
    pgn = """[Event "Rated Bullet game"]
[Site "https://lichess.org/TTYLmSUX"]
[Date "2020.08.02"]
[White "Daddy_007"]
[Black "siddhartha13"]
[Result "1-0"]
[UTCDate "2020.08.02"]
[UTCTime "04:38:18"]
[WhiteElo "1883"]
[BlackElo "1965"]
[WhiteRatingDiff "+7"]
[BlackRatingDiff "-7"]
[Variant "Standard"]
[TimeControl "60+0"]
[ECO "B21"]
[Opening "Sicilian Defense: McDonnell Attack"]
[Termination "Time forfeit"]

1. e4 { [%eval 0.05] [%clk 0:01:00] } 1... c5 { [%eval 0.32] [%clk 0:01:00] } 2. f4 { [%eval 0.0] [%clk 0:00:59] } 2... d6 { [%eval 0.25] [%clk 0:01:00] } 3. Nf3 { [%eval 0.0] [%clk 0:00:59] } 3... Nf6 { [%eval 0.09] [%clk 0:00:59] } 4. d3 { [%eval -0.09] [%clk 0:00:57] } 4... g6 { [%eval 0.2] [%clk 0:00:58] } 5. c3 { [%eval 0.0] [%clk 0:00:57] } 5... Bg7 { [%eval 0.0] [%clk 0:00:58] } 6. e5 { [%eval -0.75] [%clk 0:00:56] } 6... dxe5 { [%eval -0.76] [%clk 0:00:56] } 7. fxe5 { [%eval -0.87] [%clk 0:00:56] } 7... Nd5 { [%eval -1.1] [%clk 0:00:56] } 8. d4 { [%eval -0.78] [%clk 0:00:55] } 8... cxd4 { [%eval -0.48] [%clk 0:00:54] } 9. cxd4 { [%eval -0.6] [%clk 0:00:54] } 9... O-O { [%eval -0.69] [%clk 0:00:53] } 10. Nc3 { [%eval -0.52] [%clk 0:00:54] } 10... Nc6 { [%eval -0.5] [%clk 0:00:53] } 11. Nxd5 { [%eval -1.75] [%clk 0:00:53] } 11... Qxd5 { [%eval -1.57] [%clk 0:00:52] } 12. Be3 { [%eval -1.59] [%clk 0:00:52] } 12... Bg4 { [%eval -1.14] [%clk 0:00:50] } 13. Be2 { [%eval -1.02] [%clk 0:00:50] } 13... Bxf3 { [%eval -0.28] [%clk 0:00:49] } 14. Bxf3 { [%eval -0.09] [%clk 0:00:50] } 14... Qa5+ { [%eval -0.14] [%clk 0:00:48] } 15. Bd2 { [%eval -1.25] [%clk 0:00:48] } 15... Qb5 { [%eval -0.44] [%clk 0:00:46] } 16. Bc3 { [%eval -0.36] [%clk 0:00:47] } 16... Rad8 { [%eval -0.33] [%clk 0:00:42] } 17. Be2 { [%eval -3.07] [%clk 0:00:45] } 17... Qb6 { [%eval -3.17] [%clk 0:00:40] } 18. d5 { [%eval -5.21] [%clk 0:00:40] } 18... Nxe5 { [%eval -4.54] [%clk 0:00:38] } 19. Bxe5 { [%eval -6.68] [%clk 0:00:39] } 19... Bxe5 { [%eval -6.39] [%clk 0:00:37] } 20. Qd3 { [%eval -6.93] [%clk 0:00:38] } 20... Qxb2 { [%eval -6.11] [%clk 0:00:34] } 21. O-O { [%eval -6.83] [%clk 0:00:37] } 21... Qd4+ { [%eval -7.05] [%clk 0:00:29] } 22. Kh1 { [%eval -7.13] [%clk 0:00:36] } 22... Qxd3 { [%eval -7.36] [%clk 0:00:28] } 23. Bxd3 { [%eval -7.23] [%clk 0:00:36] } 23... Bxa1 { [%eval -6.76] [%clk 0:00:27] } 24. Rxa1 { [%eval -7.07] [%clk 0:00:34] } 24... Rxd5 { [%eval -7.33] [%clk 0:00:27] } 25. Bc4 { [%eval -7.39] [%clk 0:00:33] } 25... Rd4 { [%eval -7.76] [%clk 0:00:26] } 26. Bb3 { [%eval -8.07] [%clk 0:00:33] } 26... Rfd8 { [%eval -8.06] [%clk 0:00:25] } 27. Rf1 { [%eval -8.37] [%clk 0:00:32] } 27... Rd2 { [%eval -6.99] [%clk 0:00:24] } 28. h3 { [%eval -8.01] [%clk 0:00:31] } 28... Rb2 { [%eval -7.4] [%clk 0:00:23] } 29. Bxf7+ { [%eval -7.47] [%clk 0:00:29] } 29... Kg7 { [%eval -7.56] [%clk 0:00:22] } 30. Bb3 { [%eval -7.8] [%clk 0:00:29] } 30... Rdd2 { [%eval -8.0] [%clk 0:00:21] } 31. Rg1 { [%eval -7.94] [%clk 0:00:27] } 31... b5 { [%eval -8.19] [%clk 0:00:17] } 32. Kh2 { [%eval -8.65] [%clk 0:00:26] } 32... a5 { [%eval -8.25] [%clk 0:00:16] } 33. Be6 { [%eval -9.0] [%clk 0:00:24] } 33... a4 { [%eval -9.12] [%clk 0:00:15] } 34. Kg3 { [%eval -9.26] [%clk 0:00:22] } 34... Kf6 { [%eval -9.12] [%clk 0:00:14] } 35. Bd7 { [%eval -10.57] [%clk 0:00:21] } 35... Rxa2 { [%eval -7.94] [%clk 0:00:13] } 36. Bc6 { [%eval -9.57] [%clk 0:00:20] } 36... Ke5 { [%eval -8.0] [%clk 0:00:12] } 37. Bxb5 { [%eval -7.94] [%clk 0:00:19] } 37... Kd4 { [%eval -7.05] [%clk 0:00:10] } 38. Re1 { [%eval -9.37] [%clk 0:00:17] } 38... e5 { [%eval -7.94] [%clk 0:00:10] } 39. Bc6 { [%eval -7.74] [%clk 0:00:14] } 39... Ra3+ { [%eval -8.25] [%clk 0:00:09] } 40. Kh2 { [%eval -7.08] [%clk 0:00:13] } 40... Raa2 { [%eval -7.17] [%clk 0:00:07] } 41. Bf3 { [%eval -7.78] [%clk 0:00:12] } 41... Kc3 { [%eval -5.91] [%clk 0:00:06] } 42. Re4 { [%eval -6.22] [%clk 0:00:12] } 42... a3 { [%eval -5.89] [%clk 0:00:05] } 43. Re3+ { [%eval -10.01] [%clk 0:00:11] } 43... Kb4 { [%eval -6.35] [%clk 0:00:05] } 44. Re4+ { [%eval -8.42] [%clk 0:00:10] } 44... Kb3 { [%eval -6.06] [%clk 0:00:04] } 45. Re3+ { [%eval -7.02] [%clk 0:00:09] } 45... Kb2 { [%eval -5.79] [%clk 0:00:04] } 46. Rxe5 { [%eval -5.98] [%clk 0:00:09] } 46... Ra1 { [%eval -5.54] [%clk 0:00:03] } 47. Re3 { [%eval -9.78] [%clk 0:00:07] } 47... Rc2 { [%eval -5.1] [%clk 0:00:02] } 48. Bd5 { [%eval -5.67] [%clk 0:00:07] } 48... a2 { [%eval -3.56] [%clk 0:00:01] } 49. Rb3+ { [%eval -3.39] [%clk 0:00:06] } 49... Kc1 { [%eval #3] [%clk 0:00:00] } 50. Ra3 { [%eval #-3] [%clk 0:00:05] } 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.EvalsVisitor(game))

    assert game.evals == ['0.05', '0.32', '0.0', '0.25', '0.0', '0.09',
                          '-0.09', '0.2', '0.0', '0.0', '-0.75', '-0.76',
                          '-0.87', '-1.1', '-0.78', '-0.48', '-0.6', '-0.69',
                          '-0.52', '-0.5', '-1.75', '-1.57', '-1.59', '-1.14',
                          '-1.02', '-0.28', '-0.09', '-0.14', '-1.25', '-0.44',
                          '-0.36', '-0.33', '-3.07', '-3.17', '-5.21', '-4.54',
                          '-6.68', '-6.39', '-6.93', '-6.11', '-6.83', '-7.05',
                          '-7.13', '-7.36', '-7.23', '-6.76', '-7.07', '-7.33',
                          '-7.39', '-7.76', '-8.07', '-8.06', '-8.37', '-6.99',
                          '-8.01', '-7.4', '-7.47', '-7.56', '-7.8', '-8.0',
                          '-7.94', '-8.19', '-8.65', '-8.25', '-9.0', '-9.12',
                          '-9.26', '-9.12', '-10.57', '-7.94', '-9.57', '-8.0',
                          '-7.94', '-7.05', '-9.37', '-7.94', '-7.74', '-8.25',
                          '-7.08', '-7.17', '-7.78', '-5.91', '-6.22', '-5.89',
                          '-10.01', '-6.35', '-8.42', '-6.06', '-7.02',
                          '-5.79', '-5.98', '-5.54', '-9.78', '-5.1', '-5.67',
                          '-3.56', '-3.39', '9999', '-9999',
                          ]
    assert all([x == 20 for x in game.eval_depths])


def test_clocks_visitor():
    pgn = """[Event "Rated Bullet game"]
[Site "https://lichess.org/TTYLmSUX"]
[Date "2020.08.02"]
[White "Daddy_007"]
[Black "siddhartha13"]
[Result "1-0"]
[UTCDate "2020.08.02"]
[UTCTime "04:38:18"]
[WhiteElo "1883"]
[BlackElo "1965"]
[WhiteRatingDiff "+7"]
[BlackRatingDiff "-7"]
[Variant "Standard"]
[TimeControl "60+0"]
[ECO "B21"]
[Opening "Sicilian Defense: McDonnell Attack"]
[Termination "Time forfeit"]

1. e4 { [%eval 0.05] [%clk 0:01:00] } 1... c5 { [%eval 0.32] [%clk 0:01:00] } 2. f4 { [%eval 0.0] [%clk 0:00:59] } 2... d6 { [%eval 0.25] [%clk 0:01:00] } 3. Nf3 { [%eval 0.0] [%clk 0:00:59] } 3... Nf6 { [%eval 0.09] [%clk 0:00:59] } 4. d3 { [%eval -0.09] [%clk 0:00:57] } 4... g6 { [%eval 0.2] [%clk 0:00:58] } 5. c3 { [%eval 0.0] [%clk 0:00:57] } 5... Bg7 { [%eval 0.0] [%clk 0:00:58] } 6. e5 { [%eval -0.75] [%clk 0:00:56] } 6... dxe5 { [%eval -0.76] [%clk 0:00:56] } 7. fxe5 { [%eval -0.87] [%clk 0:00:56] } 7... Nd5 { [%eval -1.1] [%clk 0:00:56] } 8. d4 { [%eval -0.78] [%clk 0:00:55] } 8... cxd4 { [%eval -0.48] [%clk 0:00:54] } 9. cxd4 { [%eval -0.6] [%clk 0:00:54] } 9... O-O { [%eval -0.69] [%clk 0:00:53] } 10. Nc3 { [%eval -0.52] [%clk 0:00:54] } 10... Nc6 { [%eval -0.5] [%clk 0:00:53] } 11. Nxd5 { [%eval -1.75] [%clk 0:00:53] } 11... Qxd5 { [%eval -1.57] [%clk 0:00:52] } 12. Be3 { [%eval -1.59] [%clk 0:00:52] } 12... Bg4 { [%eval -1.14] [%clk 0:00:50] } 13. Be2 { [%eval -1.02] [%clk 0:00:50] } 13... Bxf3 { [%eval -0.28] [%clk 0:00:49] } 14. Bxf3 { [%eval -0.09] [%clk 0:00:50] } 14... Qa5+ { [%eval -0.14] [%clk 0:00:48] } 15. Bd2 { [%eval -1.25] [%clk 0:00:48] } 15... Qb5 { [%eval -0.44] [%clk 0:00:46] } 16. Bc3 { [%eval -0.36] [%clk 0:00:47] } 16... Rad8 { [%eval -0.33] [%clk 0:00:42] } 17. Be2 { [%eval -3.07] [%clk 0:00:45] } 17... Qb6 { [%eval -3.17] [%clk 0:00:40] } 18. d5 { [%eval -5.21] [%clk 0:00:40] } 18... Nxe5 { [%eval -4.54] [%clk 0:00:38] } 19. Bxe5 { [%eval -6.68] [%clk 0:00:39] } 19... Bxe5 { [%eval -6.39] [%clk 0:00:37] } 20. Qd3 { [%eval -6.93] [%clk 0:00:38] } 20... Qxb2 { [%eval -6.11] [%clk 0:00:34] } 21. O-O { [%eval -6.83] [%clk 0:00:37] } 21... Qd4+ { [%eval -7.05] [%clk 0:00:29] } 22. Kh1 { [%eval -7.13] [%clk 0:00:36] } 22... Qxd3 { [%eval -7.36] [%clk 0:00:28] } 23. Bxd3 { [%eval -7.23] [%clk 0:00:36] } 23... Bxa1 { [%eval -6.76] [%clk 0:00:27] } 24. Rxa1 { [%eval -7.07] [%clk 0:00:34] } 24... Rxd5 { [%eval -7.33] [%clk 0:00:27] } 25. Bc4 { [%eval -7.39] [%clk 0:00:33] } 25... Rd4 { [%eval -7.76] [%clk 0:00:26] } 26. Bb3 { [%eval -8.07] [%clk 0:00:33] } 26... Rfd8 { [%eval -8.06] [%clk 0:00:25] } 27. Rf1 { [%eval -8.37] [%clk 0:00:32] } 27... Rd2 { [%eval -6.99] [%clk 0:00:24] } 28. h3 { [%eval -8.01] [%clk 0:00:31] } 28... Rb2 { [%eval -7.4] [%clk 0:00:23] } 29. Bxf7+ { [%eval -7.47] [%clk 0:00:29] } 29... Kg7 { [%eval -7.56] [%clk 0:00:22] } 30. Bb3 { [%eval -7.8] [%clk 0:00:29] } 30... Rdd2 { [%eval -8.0] [%clk 0:00:21] } 31. Rg1 { [%eval -7.94] [%clk 0:00:27] } 31... b5 { [%eval -8.19] [%clk 0:00:17] } 32. Kh2 { [%eval -8.65] [%clk 0:00:26] } 32... a5 { [%eval -8.25] [%clk 0:00:16] } 33. Be6 { [%eval -9.0] [%clk 0:00:24] } 33... a4 { [%eval -9.12] [%clk 0:00:15] } 34. Kg3 { [%eval -9.26] [%clk 0:00:22] } 34... Kf6 { [%eval -9.12] [%clk 0:00:14] } 35. Bd7 { [%eval -10.57] [%clk 0:00:21] } 35... Rxa2 { [%eval -7.94] [%clk 0:00:13] } 36. Bc6 { [%eval -9.57] [%clk 0:00:20] } 36... Ke5 { [%eval -8.0] [%clk 0:00:12] } 37. Bxb5 { [%eval -7.94] [%clk 0:00:19] } 37... Kd4 { [%eval -7.05] [%clk 0:00:10] } 38. Re1 { [%eval -9.37] [%clk 0:00:17] } 38... e5 { [%eval -7.94] [%clk 0:00:10] } 39. Bc6 { [%eval -7.74] [%clk 0:00:14] } 39... Ra3+ { [%eval -8.25] [%clk 0:00:09] } 40. Kh2 { [%eval -7.08] [%clk 0:00:13] } 40... Raa2 { [%eval -7.17] [%clk 0:00:07] } 41. Bf3 { [%eval -7.78] [%clk 0:00:12] } 41... Kc3 { [%eval -5.91] [%clk 0:00:06] } 42. Re4 { [%eval -6.22] [%clk 0:00:12] } 42... a3 { [%eval -5.89] [%clk 0:00:05] } 43. Re3+ { [%eval -10.01] [%clk 0:00:11] } 43... Kb4 { [%eval -6.35] [%clk 0:00:05] } 44. Re4+ { [%eval -8.42] [%clk 0:00:10] } 44... Kb3 { [%eval -6.06] [%clk 0:00:04] } 45. Re3+ { [%eval -7.02] [%clk 0:00:09] } 45... Kb2 { [%eval -5.79] [%clk 0:00:04] } 46. Rxe5 { [%eval -5.98] [%clk 0:00:09] } 46... Ra1 { [%eval -5.54] [%clk 0:00:03] } 47. Re3 { [%eval -9.78] [%clk 0:00:07] } 47... Rc2 { [%eval -5.1] [%clk 0:00:02] } 48. Bd5 { [%eval -5.67] [%clk 0:00:07] } 48... a2 { [%eval -3.56] [%clk 0:00:01] } 49. Rb3+ { [%eval -3.39] [%clk 0:00:06] } 49... Kc1 { [%eval #3] [%clk 0:00:00] } 50. Ra3 { [%eval #-3] [%clk 0:00:05] } 1-0"""  # noqa

    game = chess.pgn.read_game(io.StringIO(pgn))

    game.accept(visitors.ClocksVisitor(game))

    assert game.clocks == ['0:01:00', '0:01:00', '0:00:59', '0:01:00',
                           '0:00:59', '0:00:59', '0:00:57', '0:00:58',
                           '0:00:57', '0:00:58', '0:00:56', '0:00:56',
                           '0:00:56', '0:00:56', '0:00:55', '0:00:54',
                           '0:00:54', '0:00:53', '0:00:54', '0:00:53',
                           '0:00:53', '0:00:52', '0:00:52', '0:00:50',
                           '0:00:50', '0:00:49', '0:00:50', '0:00:48',
                           '0:00:48', '0:00:46', '0:00:47', '0:00:42',
                           '0:00:45', '0:00:40', '0:00:40', '0:00:38',
                           '0:00:39', '0:00:37', '0:00:38', '0:00:34',
                           '0:00:37', '0:00:29', '0:00:36', '0:00:28',
                           '0:00:36', '0:00:27', '0:00:34', '0:00:27',
                           '0:00:33', '0:00:26', '0:00:33', '0:00:25',
                           '0:00:32', '0:00:24', '0:00:31', '0:00:23',
                           '0:00:29', '0:00:22', '0:00:29', '0:00:21',
                           '0:00:27', '0:00:17', '0:00:26', '0:00:16',
                           '0:00:24', '0:00:15', '0:00:22', '0:00:14',
                           '0:00:21', '0:00:13', '0:00:20', '0:00:12',
                           '0:00:19', '0:00:10', '0:00:17', '0:00:10',
                           '0:00:14', '0:00:09', '0:00:13', '0:00:07',
                           '0:00:12', '0:00:06', '0:00:12', '0:00:05',
                           '0:00:11', '0:00:05', '0:00:10', '0:00:04',
                           '0:00:09', '0:00:04', '0:00:09', '0:00:03',
                           '0:00:07', '0:00:02', '0:00:07', '0:00:01',
                           '0:00:06', '0:00:00', '0:00:05',
                           ]


def test_queen_exchange_visitor():
    pgn = """[Event "Rated Bullet game"]
[Site "https://lichess.org/TTYLmSUX"]
[Date "2020.08.02"]
[White "Daddy_007"]
[Black "siddhartha13"]
[Result "1-0"]
[UTCDate "2020.08.02"]
[UTCTime "04:38:18"]
[WhiteElo "1883"]
[BlackElo "1965"]
[WhiteRatingDiff "+7"]
[BlackRatingDiff "-7"]
[Variant "Standard"]
[TimeControl "60+0"]
[ECO "B21"]
[Opening "Sicilian Defense: McDonnell Attack"]
[Termination "Time forfeit"]

1. e4 c5 2. f4 d6 3. Nf3 Nf6 4. d3 g6 5. c3 Bg7 6. e5 dxe5 7. fxe5 Nd5 8. d4 cxd4 9. cxd4 O-O 10. Nc3 Nc6 11. Nxd5 Qxd5 12. Be3 Bg4 13. Be2 Bxf3 14. Bxf3 Qa5+ 15. Bd2 Qb5 16. Bc3 Rad8 17. Be2 Qb6 18. d5 Nxe5 19. Bxe5 Bxe5 20. Qd3 Qxb2 21. O-O Qd4+ 22. Kh1 Qxd3 23. Bxd3 Bxa1 24. Rxa1 Rxd5 25. Bc4 Rd4 26. Bb3 Rfd8 27. Rf1 Rd2 28. h3 Rb2 29. Bxf7+ Kg7 30. Bb3 Rdd2 31. Rg1 b5 32. Kh2 a5 33. Be6 a4 34. Kg3 Kf6 35. Bd7 Rxa2 36. Bc6 Ke5 37. Bxb5 Kd4 38. Re1 e5 39. Bc6 Ra3+ 40. Kh2 Raa2 41. Bf3 Kc3 42. Re4 a3 43. Re3+ Kb4 44. Re4+ Kb3 45. Re3+ Kb2 46. Rxe5 Ra1 47. Re3 Rc2 48. Bd5 a2 49. Rb3+ Kc1 50. Ra3 1-0"""  # noqa

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

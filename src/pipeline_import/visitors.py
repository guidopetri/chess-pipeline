#! /usr/bin/env python

import re
from collections import Counter
from datetime import datetime

import chess
from chess.pgn import BaseVisitor


class EvalsVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.headers._others['evaluations'] = []
        self.game.headers._others['eval_depths'] = []

    def visit_comment(self, comment):
        if (eval_match := re.search(r'\[%eval ([^\]]+)', comment)) is not None:
            evaluation = eval_match.group(1)

            # if it's a checkmate sequence
            if evaluation.startswith('#'):
                # if it's a checkmate for black, it'll be e.g. #-30
                if '-' in evaluation:
                    evaluation = '-9999'
                else:  # otherwise it's for white, e.g. #30
                    evaluation = '9999'
            self.game.headers._others['evaluations'].append(float(evaluation))
            self.game.headers._others['eval_depths'].append(20)
        elif self.game.headers._others['evaluations']:
            # if we have evaluations in general but not for this position
            # we are usually in a checkmate position
            # these don't show up as evals in the PGN :/

            # we don't have to consider stalemate because this shows up
            # as an eval of 0.0
            results_map = {'1-0': 9999.0,
                           '0-1': -9999.0,
                           }

            result = results_map[self.game.headers['Result']]

            self.game.headers._others['evaluations'].append(result)
            self.game.headers._others['eval_depths'].append(20)

    def result(self):
        return None


class ClocksVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.headers._others['clocks'] = []
        self.game.headers._others['white_berserked'] = False
        self.game.headers._others['black_berserked'] = False

    def visit_comment(self, comment):
        if (clock_match := re.search(r'\[%clk ([^\]]+)', comment)) is not None:
            clock_time = clock_match.group(1)
        else:
            clock_time = ''

        # berserked games stuff
        if len(self.game.headers._others['clocks']) == 0:
            self.white_clock = datetime.strptime(clock_time, '%H:%M:%S')
        elif len(self.game.headers._others['clocks']) == 1:
            self.black_clock = datetime.strptime(clock_time, '%H:%M:%S')
        elif len(self.game.headers._others['clocks']) == 2:
            if self.black_clock > self.white_clock:
                self.game.headers._others['white_berserked'] = True
            elif self.white_clock > self.black_clock:
                self.game.headers._others['black_berserked'] = True

        self.game.headers._others['clocks'].append(clock_time)

    def result(self):
        return None


class QueenExchangeVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm

    def begin_game(self):
        self.move_counter = 0
        self.captured_at = 0
        self.game.headers._others['queen_exchange'] = False

    def visit_move(self, board, move):
        self.move_counter += 1
        dest = board.piece_at(move.to_square)
        if dest is not None and dest.piece_type == chess.QUEEN:
            if self.captured_at == self.move_counter - 1:
                self.game.headers._others['queen_exchange'] = True
            self.captured_at = self.move_counter

    def result(self):
        return None


class CastlingVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm

    def begin_game(self):
        self.game.headers._others['castling_sides'] = {'black': None,
                                                       'white': None,
                                                       }

    def visit_move(self, board, move):
        from_sq = board.piece_at(move.from_square)
        if from_sq is not None and from_sq.piece_type == chess.KING:
            if move.to_square == chess.G8:
                self.game.headers._others['castling_sides']['black'] = 'kingside'  # noqa
            elif move.to_square == chess.G1:
                self.game.headers._others['castling_sides']['white'] = 'kingside'  # noqa
            elif move.to_square == chess.C8:
                self.game.headers._others['castling_sides']['black'] = 'queenside'  # noqa
            elif move.to_square == chess.C1:
                self.game.headers._others['castling_sides']['white'] = 'queenside'  # noqa

    def result(self):
        return None


class PositionsVisitor(BaseVisitor):

    def __init__(self, game):
        self.game = game
        self.game.headers._others['positions'] = []
        self.first_move = True

    def visit_board(self, board):
        if not self.first_move:
            self.game.headers._others['positions'].append(board.fen())
        self.first_move = False

    def result(self):
        return None


class PromotionsVisitor(BaseVisitor):

    def __init__(self, gm):
        self.gm = gm

    def begin_game(self):
        self.gm.headers._others['has_promotion'] = False
        self.gm.headers._others['promotion_count'] = {'white': 0,
                                                      'black': 0,
                                                      }
        self.gm.headers._others['promotions'] = {'white': [],
                                                 'black': [],
                                                 }

    def visit_move(self, board, move):
        if move.promotion is not None:
            self.gm.headers._others['has_promotion'] = True
            self.gm.headers._others['promotion_count'][board.turn] += 1

            piece_symbol = chess.piece_symbol(move.promotion)
            self.gm.headers._others['promotions'][board.turn].append(piece_symbol)  # noqa

    def end_game(self):
        self.gm.headers._others['promotion_count_white'] = self.gm.headers._others['promotion_count']['white']  # noqa
        self.gm.headers._others['promotion_count_black'] = self.gm.headers._others['promotion_count']['black']  # noqa

        promotions = sorted(self.gm.headers._others['promotions']['white'])
        self.gm.headers._others['promotions_white'] = ''.join(promotions)

        promotions = sorted(self.gm.headers._others['promotions']['black'])
        self.gm.headers._others['promotions_black'] = ''.join(promotions)

    def result(self):
        return None


class MaterialVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.headers._others['material_by_move'] = []

    def visit_board(self, board):
        pieces = board.piece_map()
        symbols = [v.symbol() for k, v in pieces.items()]

        summary = Counter(symbols)
        self.game.headers._others['material_by_move'].append(summary)

    def result(self):
        return None

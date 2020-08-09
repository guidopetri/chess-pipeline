#! /usr/bin/env python

from chess.pgn import BaseVisitor
import chess
import re
from datetime import datetime
from collections import Counter


class EvalsVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.evals = []
        self.game.eval_depths = []

    def visit_comment(self, comment):
        if 'eval' in comment:
            evaluation = re.search(r'\[%eval ([^\]]+)', comment).group(1)

            # if it's a checkmate sequence
            if evaluation.startswith('#'):
                # if it's a checkmate for black, it'll be e.g. #-30
                if '-' in evaluation:
                    evaluation = -9999
                else:  # otherwise it's for white, e.g. #30
                    evaluation = 9999
            self.game.evals.append(evaluation)
            self.game.eval_depths.append(20)

    def result(self):
        return None


class ClocksVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.clocks = []
        self.game.white_berserked = False
        self.game.black_berserked = False

    def visit_comment(self, comment):
        if 'clk' in comment:
            clock_time = re.search(r'\[%clk ([^\]]+)', comment).group(1)
        else:
            clock_time = ''

        # berserked games stuff
        if len(self.game.clocks) == 0:
            self.white_clock = datetime.strptime(clock_time, '%H:%M:%S')
        elif len(self.game.clocks) == 1:
            self.black_clock = datetime.strptime(clock_time, '%H:%M:%S')
        elif len(self.game.clocks) == 2:
            if self.black_clock > self.white_clock:
                self.game.white_berserked = True
            elif self.white_clock > self.black_clock:
                self.game.black_berserked = True

        self.game.clocks.append(clock_time)

    def result(self):
        return None


class QueenExchangeVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm

    def begin_game(self):
        self.move_counter = 0
        self.captured_at = 0
        self.game.queen_exchange = False

    def visit_move(self, board, move):
        self.move_counter += 1
        dest = board.piece_at(move.to_square)
        if dest is not None and dest.piece_type == chess.QUEEN:
            if self.captured_at == self.move_counter - 1:
                self.game.queen_exchange = True
            self.captured_at = self.move_counter

    def result(self):
        return None


class CastlingVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm

    def begin_game(self):
        self.game.castling = {'black': None,
                              'white': None,
                              }

    def visit_move(self, board, move):
        from_sq = board.piece_at(move.from_square)
        if from_sq is not None and from_sq.piece_type == chess.KING:
            if move.to_square == chess.G8:
                self.game.castling['black'] = 'kingside'
            elif move.to_square == chess.G1:
                self.game.castling['white'] = 'kingside'
            elif move.to_square == chess.C8:
                self.game.castling['black'] = 'queenside'
            elif move.to_square == chess.C1:
                self.game.castling['white'] = 'queenside'

    def result(self):
        return None


class PositionsVisitor(BaseVisitor):

    def __init__(self, game):
        self.game = game
        self.game.positions = []

    def visit_board(self, board):
        self.game.positions.append(board.fen())

    def result(self):
        return None


class PromotionsVisitor(BaseVisitor):

    def __init__(self, gm):
        self.gm = gm

    def begin_game(self):
        self.gm.has_promotion = False
        self.gm.promotion_count = {chess.WHITE: 0,
                                   chess.BLACK: 0,
                                   }
        self.gm.promotions = {chess.WHITE: [],
                              chess.BLACK: [],
                              }

    def visit_move(self, board, move):
        if move.promotion is not None:
            self.gm.has_promotion = True
            self.gm.promotion_count[board.turn] += 1

            piece_symbol = chess.piece_symbol(move.promotion)
            self.gm.promotions[board.turn].append(piece_symbol)

    def end_game(self):
        self.gm.promotion_count_white = self.gm.promotion_count[chess.WHITE]
        self.gm.promotion_count_black = self.gm.promotion_count[chess.BLACK]

        self.gm.promotions_white = sorted(self.gm.promotions[chess.WHITE])
        self.gm.promotions_white = ''.join(self.gm.promotions_white)

        self.gm.promotions_black = sorted(self.gm.promotions[chess.BLACK])
        self.gm.promotions_black = ''.join(self.gm.promotions_black)

    def result(self):
        return None


class MaterialVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.material_by_move = []

    def visit_board(self, board):
        pieces = board.piece_map()
        symbols = [v.symbol() for k, v in pieces.items()]

        summary = Counter(symbols)
        self.game.material_by_move.append(summary)

    def result(self):
        return None

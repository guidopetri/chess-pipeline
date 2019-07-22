#! /usr/bin/env python

from chess.pgn import BaseVisitor
import re


class EvalsVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.evals = []

    def visit_comment(self, comment):
        if 'eval' in comment:
            evaluation = re.search(r'\[%eval ([^\]]+)', comment).group(1)
        else:
            evaluation = ''
        self.gm.evals.append(evaluation)


class ClocksVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm
        self.game.clocks = []

    def visit_comment(self, comment):
        if 'clk' in comment:
            clock_time = re.search(r'\[%clk ([^\]]+)', comment).group(1)
        else:
            clock_time = ''
        self.gm.clocks.append(clock_time)


class QueenExchangeVisitor(BaseVisitor):

    def __init__(self, gm):
        self.game = gm

    def begin_game(self):
        self.move_counter = 0
        self.captured_at = 0
        self.gm.queen_exchanged = False

    def visit_move(self, board, move):
        self.move_counter += 1
        dest = board.piece_at(move.to_square)
        # chess.QUEEN has a value of 5
        if dest is not None and dest.piece_type == 5:
            if self.captured_at == self.move_counter - 1:
                self.gm.queen_exchanged = True
            self.captured_at = self.move_counter

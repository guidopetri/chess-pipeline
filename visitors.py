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

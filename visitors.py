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

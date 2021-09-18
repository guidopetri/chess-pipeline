#! /usr/bin/env python

import numpy as np
from scipy.optimize import curve_fit


class tumblr_model(object):
    # reference:
    # https://chesscomputer.tumblr.com/post/98632536555/using-the-stockfish-position-evaluation-score-to
    def __init__(self, p0=[0, 0.5, 1, 1, 0], maxfev=10000):
        self.p0 = p0
        self.maxfev = maxfev

    @staticmethod
    def logifunc(x, x0, k, A, B, offset):
        return A / (B + np.exp(-k * (x - x0))) + offset

    def fit(self, x, y):
        self.popt, self.pcov = curve_fit(self.logifunc,
                                         x,
                                         y,
                                         p0=self.p0,
                                         maxfev=self.maxfev,
                                         )

    def coefs(self):
        return {x: y
                for x, y in zip(['x0', 'k', 'A', 'B', 'offset'], self.popt)
                }

    def predict(self, x):
        return self.logifunc(x, *self.popt)


class wiki_model(object):
    # reference:
    # https://www.chessprogramming.org/Pawn_Advantage,_Win_Percentage,_and_Elo
    def __init__(self):
        pass

    def predict(self, x):
        return 1 / (1 + 10 ** (-x / 4))


class leela_model(object):
    # reference: Leela Chess Zero
    def __init__(self):
        pass

    def predict(self, x):
        return ((np.arctan(x / (0.01 * 111.714640912)) / 1.5620688421) + 1) / 2

#! /usr/bin/env python3

from pipeline_import import transforms
import pandas as pd


def test_get_clean_fens():
    fen = 'r3rnk1/ppq2ppn/2pb4/3pN1p1/3P1P1B/2PB4/PPQ3PP/R3R1K1 w - - 0 19'
    clean_fen = 'r3rnk1/ppq2ppn/2pb4/3pN1p1/3P1P1B/2PB4/PPQ3PP/R3R1K1 w - - 0'

    fen = pd.Series([fen])
    clean_fen = pd.Series([clean_fen])
    assert (transforms.get_clean_fens(fen) == clean_fen).all()

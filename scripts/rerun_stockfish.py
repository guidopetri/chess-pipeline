#! /usr/bin/env python3

import multiprocessing
import stockfish
import queue
import re


def run_stockfish(fens_queue, evals_queue, stockfish_location, depth):
    sf = stockfish.Stockfish(stockfish_location, depth=depth)

    while True:
        try:
            fen = fens_queue.get(timeout=5)
        except queue.Empty:
            return

        sf.set_fen_position(fen)

        # this is copy-pasted from chess_pipeline.py; the current version of
        # the stockfish library supports a better method but i do not want
        # to upgrade yet because i am pretty sure something will break

        if sf.get_best_move() is not None:
            rating_match = re.search(r'score (cp|mate) (.+?)(?: |$)',
                                     sf.info)

            if rating_match.group(1) == 'mate':
                original_rating = int(rating_match.group(2))

                # adjust ratings for checkmate sequences
                if original_rating:
                    rating = 999900 * original_rating / abs(original_rating)
                elif ' w ' in fen:
                    rating = 999900
                else:
                    rating = -999900
            else:
                rating = int(rating_match.group(2))
            if ' b ' in fen:
                rating *= -1
            rating /= 100
        else:
            rating = None

        evals_queue.put((fen, str(rating)))


if __name__ == '__main__':
    import sys

    # get data from psql with:
    # \copy (select fen from position_evals)
    # to '/path/to/fens_to_analyze.csv' with csv delimiter ',';

    if len(sys.argv) < 4:
        raise ValueError('Not enough arguments: requires csv location,'
                         ' stockfish location, and stockfish depth')
    _, csv_location, stockfish_location, depth = sys.argv

    fens_queue = multiprocessing.Queue()
    evals_queue = multiprocessing.Queue()

    # load data
    with open(csv_location, 'r') as f:
        fens = f.readlines()

        for fen in fens:
            fens_queue.put(fen.strip())

    processes = {}

    for p in range(multiprocessing.cpu_count()):
        processes[p] = multiprocessing.Process(target=run_stockfish,
                                               args=(fens_queue,
                                                     evals_queue,
                                                     stockfish_location,
                                                     depth))

    for p in processes.values():
        p.start()

    for p in processes.values():
        p.join()

    with open('results.csv', 'w') as f:
        while True:
            try:
                evaluation = evals_queue.get(timeout=5)
            except queue.Empty:
                break

            f.write(','.join(evaluation) + '\n')

#! /usr/bin/env python3

import timeit
import stockfish


def run_benchmark(stockfish_location):
    times = []

    fen = 'rnb1k1nr/pp1p1ppp/1q2p3/2b5/4P3/5N2/PPP1QPPP/RNB1KB1R b KQkq - 2 5'

    for n in range(10, 26):
        sf = stockfish.Stockfish(stockfish_location, depth=n)  # noqa

        statement = f'sf.set_fen_position({fen}); sf.get_best_move()'
        time_taken = timeit.timeit(statement, number=100, globals=locals())

        print(f'Time taken for depth {n}: {time_taken:.2e}')

        times.append(time_taken)

    print(f'All times: {times}')
    return


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        raise ValueError('Must provide stockfish executable location')
    stockfish_location = sys.argv[1]

    run_benchmark(stockfish_location)

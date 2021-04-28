#! /usr/bin/env python3

from pandas import to_timedelta
import stockfish
import re


def get_sf_evaluation(fen, sf_location, sf_depth):
    sf = stockfish.Stockfish(sf_location,
                             depth=sf_depth)

    sf.set_fen_position(fen)
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

    return rating


def parse_headers(game, visitors, visitor_stats):
    game_infos = {x: y for x, y in game.headers.items()}
    if game.headers['Variant'] == 'From Position':
        game.headers['Variant'] = 'Standard'
    for visitor in visitors:
        game.accept(visitor(game))
    for k, v in visitor_stats.items():
        game_infos[k] = getattr(game, v)
    game_infos['moves'] = [x.san() for x in game.mainline()]

    return game_infos


def fix_provisional_columns(json):
    for side in ['black', 'white']:
        col = f'players_{side}_provisional'
        if col in json.columns:
            json[col].fillna(False, inplace=True)
        else:
            json[col] = False
    return json


def convert_clock_to_seconds(clocks):
    clocks = to_timedelta(clocks,
                          errors='coerce')
    clocks = clocks.dt.total_seconds()
    clocks.fillna(-1.0, inplace=True)
    clocks = clocks.astype(int)

    return clocks


def get_clean_fens(positions):
    # split, get all but last element of resulting list, then re-join
    return positions.str.split().str[:-1].str.join(' ')

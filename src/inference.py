import hashlib
import os

import pandas as pd
from pipeline_import.models import predict_wp


def estimate_win_probabilities(game_infos: pd.DataFrame,
                               evals: pd.DataFrame,
                               game_positions: pd.DataFrame,
                               game_clocks: pd.DataFrame,
                               local_stockfish: bool,
                               ) -> pd.DataFrame:
    game_infos['has_increment'] = (game_infos['increment'] > 0).astype(int)

    game_infos_cols = ['game_link',
                       'has_increment',
                       'player_color',
                       'player_elo',
                       'opponent_elo',
                       ]

    # evals isn't always populated
    df = pd.merge(game_positions, evals, on='fen', how='left')

    # if there are missing evals, set to 0 so it doesn't influence the WP
    if not local_stockfish:
        df['evaluation'].fillna(0, inplace=True)
        # this is actually kind of incorrect - evaluation was never scaled
        # so the mean isn't 0, but rather something like 0.2 probably.
        # since the LR model inputs weren't scaled in the first place,
        # i am just ignoring this for now

    df = pd.merge(df, game_clocks, on=['game_link', 'half_move'])
    df = pd.merge(df,
                  game_infos[game_infos_cols],
                  on='game_link',
                  )

    loss, draw, win = predict_wp(df)

    df['win_probability_white'] = win
    df['draw_probability'] = draw
    df['win_probability_black'] = loss

    model_path = os.path.join(os.path.dirname(__file__),
                              'pipeline_import',
                              'wp_model.pckl',
                              )

    with open(model_path, 'rb') as f:
        md5 = hashlib.md5(f.read()).hexdigest()

    df['win_prob_model_version'] = md5[:7]
    return df

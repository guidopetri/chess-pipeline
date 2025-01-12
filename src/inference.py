import hashlib
import os
from datetime import date
from pathlib import Path

import pandas as pd
from pipeline_import.models import predict_wp
from utils.output import get_output_file_prefix


def estimate_win_probabilities(player: str,
                               perf_type: str,
                               data_date: date,
                               local_stockfish: bool,
                               io_dir: Path,
                               ) -> None:
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    # TODO: rename output files / consolidate naming to single location
    game_infos = pd.read_parquet(io_dir / f'{prefix}_game_infos.parquet')
    evals = pd.read_parquet(io_dir / f'{prefix}_evals.parquet')
    positions = pd.read_parquet(io_dir / f'{prefix}_exploded_positions.parquet')  # noqa
    game_clocks = pd.read_parquet(io_dir / f'{prefix}_exploded_clocks.parquet')

    if all(df.empty for df in [game_infos, evals, positions, game_clocks]):
        game_infos.to_parquet(io_dir / f'{prefix}_win_probabilities.parquet')
        return

    game_infos['has_increment'] = (game_infos['increment'] > 0).astype(int)

    game_infos_cols = ['game_link',
                       'has_increment',
                       'player_color',
                       'player_elo',
                       'opponent_elo',
                       ]

    # evals isn't always populated
    df = pd.merge(positions, evals, on='fen', how='left')

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
    df.to_parquet(io_dir / f'{prefix}_win_probabilities.parquet')

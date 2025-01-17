import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Protocol

from feature_engineering import (
    clean_chess_df,
    explode_clocks,
    explode_materials,
    explode_moves,
    explode_positions,
)
from inference import estimate_win_probabilities
from pipeline_import.postgres_templates import (
    load_chess_games,
    load_game_materials,
    load_game_positions,
    load_move_clocks,
    load_move_list,
    load_position_evals,
    load_win_probs,
)
from pipeline_import.transforms import transform_game_data
from vendors.lichess import fetch_lichess_api_json, fetch_lichess_api_pgn
from vendors.stockfish import get_evals


class EtlStep(Protocol):
    """
    Protocol for an ETL step of the data pipeline.

    Callable. Should read inputs from and write outputs to `io_dir`.
    """

    def __call__(self,
                 player: str,
                 perf_type: str,
                 data_date: date,
                 local_stockfish: bool,
                 io_dir: Path,
                 ) -> None:
        ...


ETL_STEPS: dict[str, EtlStep] = {'fetch_json': fetch_lichess_api_json,
                                 'fetch_pgn': fetch_lichess_api_pgn,
                                 'clean_df': clean_chess_df,
                                 'get_evals': get_evals,
                                 'explode_moves': explode_moves,
                                 'explode_clocks': explode_clocks,
                                 'explode_positions': explode_positions,
                                 'explode_materials': explode_materials,
                                 'get_game_infos': transform_game_data,
                                 'get_win_probs': estimate_win_probabilities,
                                 'load_chess_games': load_chess_games,
                                 'load_position_evals': load_position_evals,
                                 'load_game_positions': load_game_positions,
                                 'load_game_materials': load_game_materials,
                                 'load_move_clocks': load_move_clocks,
                                 'load_move_list': load_move_list,
                                 'load_win_probs': load_win_probs,
                                 }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='ETL for lichess data')
    parser.add_argument('--player',
                        type=str,
                        default='thibault',
                        help='Lichess username for the player whose data will '
                             'be downloaded.',
                        )
    parser.add_argument('--perf_type',
                        type=str,
                        default='bullet',
                        choices=['ultrabullet',
                                 'bullet',
                                 'blitz',
                                 'rapid',
                                 'classical',
                                 ],
                        help='Perf type to download player data for.',
                        )
    parser.add_argument('--data_date',
                        type=date.fromisoformat,
                        default=datetime(2024, 1, 29),
                        help='Date to download data for. Works in GMT.',
                        )
    parser.add_argument('--local_stockfish',
                        action='store_true',
                        help='Whether to use stockfish locally to calculate '
                             'position evaluations.',
                        )
    parser.add_argument('--step',
                        type=str,
                        choices=ETL_STEPS.keys(),
                        required=True,
                        help='Which ETL step to run.',
                        )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    df = ETL_STEPS[args.step](player=args.player,
                              perf_type=args.perf_type,
                              data_date=args.data_date,
                              local_stockfish=args.local_stockfish,
                              io_dir=Path(os.environ['DAGSTER_IO_DIR']),
                              )

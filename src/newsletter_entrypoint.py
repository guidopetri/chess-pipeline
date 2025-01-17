#! /usr/bin/env python3

import argparse
import os
import pickle
from pathlib import Path
from typing import Protocol

import pandas as pd
from pipeline_import.transforms import (
    get_weekly_data,
)
from utils.newsletter import (
    create_newsletter,
    generate_elo_by_weekday_text,
    generate_win_ratio_by_color_text,
    send_newsletter,
)


class Step(Protocol):
    """
    Protocol for a data processing step of the newsletter pipeline.

    Callable. Should read inputs from and write outputs to `io_dir`.
    """

    def __call__(self,
                 player: str,
                 category: str,
                 receiver: str,
                 io_dir: Path,
                 ) -> None:
        ...


def get_data(player: str,
             category: str,
             receiver: str,
             io_dir: Path,
             ) -> None:
    df = get_weekly_data(player)
    df.to_parquet(io_dir / f'week-data-{player}.parquet')


def win_ratio_by_color(player: str,
                       category: str,
                       receiver: str,
                       io_dir: Path,
                       ) -> None:
    df = pd.read_parquet(io_dir / f'week-data-{player}.parquet')
    text = generate_win_ratio_by_color_text(df, player, io_dir=io_dir)
    target_path = io_dir / f'win-by-color-{player}.txt'
    target_path.write_text(text)


def elo_by_weekday(player: str,
                   category: str,
                   receiver: str,
                   io_dir: Path,
                   ) -> None:
    df = pd.read_parquet(io_dir / f'week-data-{player}.parquet')
    text = generate_elo_by_weekday_text(df, category, player, io_dir=io_dir)
    target_path = io_dir / f'elo-by-weekday-{player}.txt'
    target_path.write_text(text)


def create_email(player: str,
                 category: str,
                 receiver: str,
                 io_dir: Path,
                 ) -> None:
    input_paths = [f'win-by-color-{player}.txt',
                   f'elo-by-weekday-{player}.txt',
                   ]
    texts = [(io_dir / p).read_text() for p in input_paths]
    newsletter = create_newsletter(texts=texts,
                                   player=player,
                                   receiver=receiver,
                                   io_dir=io_dir,
                                   )
    target_path = io_dir / f'newsletter-{player}.pckl'
    with open(target_path, 'wb') as f:
        pickle.dump(newsletter, f)


def send_email(player: str,
               category: str,
               receiver: str,
               io_dir: Path,
               ) -> None:
    source_path = io_dir / f'newsletter-{player}.pckl'
    with open(source_path, 'rb') as f:
        newsletter = pickle.load(f)
    send_newsletter(newsletter)


STEPS: dict[str, Step] = {'get_data': get_data,
                          'win_ratio_by_color': win_ratio_by_color,
                          'elo_by_weekday': elo_by_weekday,
                          'create_email': create_email,
                          'send_email': send_email,
                          }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Newsletter for chess data')
    parser.add_argument('--player',
                        type=str,
                        default='thibault',
                        help='Lichess username for the player whose data will '
                             'be sent.',
                        )
    parser.add_argument('--category',
                        type=str,
                        default='blitz',
                        choices=['ultrabullet',
                                 'bullet',
                                 'blitz',
                                 'rapid',
                                 'classical',
                                 ],
                        help='Chess category to send newsletter for.',
                        )
    parser.add_argument('--receiver',
                        type=str,
                        required=True,
                        help='Email to send newsletter to.',
                        )
    parser.add_argument('--step',
                        type=str,
                        choices=STEPS.keys(),
                        required=True,
                        help='Which newsletter processing step to run.',
                        )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    STEPS[args.step](player=args.player,
                     category=args.category,
                     receiver=args.receiver,
                     io_dir=Path(os.environ['DAGSTER_IO_DIR']),
                     )

"""
Generic utils.

TODO: move to a better location.
"""

from datetime import date


def get_output_file_prefix(player: str,
                           perf_type: str,
                           data_date: date,
                           ) -> str:
    prefix = f'{data_date.strftime("%F")}_{player}_{perf_type}'
    return prefix

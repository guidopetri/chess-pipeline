import os
from datetime import date
from pathlib import Path

import pandas as pd
import valkey
from pipeline_import.configs import get_cfg
from pipeline_import.transforms import get_clean_fens, get_sf_evaluation
from utils.db import run_remote_sql_query


def get_evals(player: str,
              perf_type: str,
              data_date: date,
              local_stockfish: bool,
              io_dir: Path,
              ) -> None:
    df = pd.read_parquet(io_dir / 'cleaned_df.parquet')

    sf_params = get_cfg('stockfish_cfg')

    df = df[['evaluations', 'eval_depths', 'positions']]

    # explode the two different list-likes separately, then concat
    no_evals: pd.DataFrame = df[~df['evaluations'].map(any)]
    df = df[df['evaluations'].map(any)]

    no_evals = pd.DataFrame(no_evals['positions'].explode())
    no_evals['positions'] = get_clean_fens(no_evals['positions'])

    evals: pd.Series = df['evaluations'].explode().reset_index(drop=True)
    depths: pd.Series = df['eval_depths'].explode().reset_index(drop=True)
    positions: pd.Series = df['positions'].explode().reset_index(drop=True)
    positions = get_clean_fens(positions)

    sql: str = """SELECT fen, evaluation, eval_depth
                  FROM position_evals
                  WHERE fen IN %(positions)s;
                  """
    db_evaluations = run_remote_sql_query(sql,
                                          positions=tuple(positions.tolist() + no_evals['positions'].tolist()),  # noqa
                                          )
    positions_evaluated = db_evaluations['fen'].drop_duplicates()

    df = pd.concat([positions, evals, depths], axis=1)

    if local_stockfish:

        local_evals: list[float | None] = []

        counter: int = 0
        position_count: int = len(no_evals['positions'])
        evaluation: float | None = None

        valkey_url: str = os.environ['VALKEY_CONNECTION_URL']
        valkey_client: valkey.Valkey = valkey.from_url(valkey_url,
                                                       decode_responses=True,
                                                       )

        for position in no_evals['positions'].tolist():
            if position in positions_evaluated.values:
                # position will be dropped later if evaluation is None
                evaluation = None
            else:
                evaluation = get_sf_evaluation(position + ' 0',
                                               Path(sf_params['location']),
                                               int(sf_params['depth']),
                                               valkey_client,
                                               )

            local_evals.append(evaluation)

            # progress bar stuff
            counter += 1

            current_progress = counter / position_count
            print(f'Analyzed :: {counter} / {position_count} '
                  f':: {current_progress:.2%}')

        print(f'Analyzed all {position_count} positions')

        no_evals['evaluations'] = local_evals
        no_evals['eval_depths'] = sf_params['depth']
        no_evals.dropna(inplace=True)

        df = pd.concat([df, no_evals], axis=0, ignore_index=True)

    df = df[~df['positions'].isin(positions_evaluated)]

    df.rename(columns={'evaluations': 'evaluation',
                       'eval_depths': 'eval_depth',
                       'positions': 'fen'},
              inplace=True)
    df['evaluation'] = pd.to_numeric(df['evaluation'],
                                     errors='coerce')
    df['eval_depth'] = pd.to_numeric(df['eval_depth'])

    df.dropna(inplace=True)

    if not db_evaluations.empty:
        df = pd.concat([df, db_evaluations], axis=0, ignore_index=True)

    df.to_parquet(io_dir / 'evals.parquet')

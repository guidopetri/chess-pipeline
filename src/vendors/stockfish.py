from typing import Any

import pandas as pd
from luigi import Task
from pipeline_import.configs import stockfish_cfg
from pipeline_import.transforms import get_clean_fens, get_sf_evaluation
from utils.db import run_remote_sql_query


def get_evals(df: pd.DataFrame,
              local_stockfish: bool,
              task: Task,
              ) -> pd.DataFrame:
    sf_params: Any = stockfish_cfg()

    df = df[['evaluations', 'eval_depths', 'positions']]

    # explode the two different list-likes separately, then concat
    no_evals: pd.DataFrame = df[~df['evaluations'].astype(bool)]
    df = df[df['evaluations'].astype(bool)]

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

        for position in no_evals['positions'].tolist():
            if position in positions_evaluated.values:
                # position will be dropped later if evaluation is None
                evaluation = None
            else:
                sf_eval: float | None = get_sf_evaluation(position + ' 0',
                                                          sf_params.location,
                                                          sf_params.depth)
                if sf_eval is not None:
                    # TODO: this is implicitly setting evaluation = last
                    # eval if in a checkmate position. handle this better
                    evaluation = sf_eval

            local_evals.append(evaluation)

            # progress bar stuff
            counter += 1

            current_progress = counter / position_count
            task.set_status_message(f'Analyzed :: '
                                    f'{counter} / {position_count}')
            task.set_progress_percentage(round(current_progress * 100, 2))

        task.set_status_message(f'Analyzed all {position_count} positions')
        task.set_progress_percentage(100)

        no_evals['evaluations'] = local_evals
        no_evals['eval_depths'] = sf_params.depth
        no_evals.dropna(inplace=True)

        df = pd.concat([df, no_evals], axis=0, ignore_index=True)

    df = df[~df['positions'].isin(positions_evaluated)]

    df.rename(columns={'evaluations': 'evaluation',
                       'eval_depths': 'eval_depth',
                       'positions': 'fen'},
              inplace=True)
    df['evaluation'] = pd.to_numeric(df['evaluation'],
                                     errors='coerce')

    df.dropna(inplace=True)
    df = pd.concat([df, db_evaluations], axis=0, ignore_index=True)

    return df

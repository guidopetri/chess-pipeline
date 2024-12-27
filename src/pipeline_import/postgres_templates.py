#! /usr/bin/env python3


from datetime import date
from pathlib import Path

import adbc_driver_postgresql.dbapi
import pyarrow.parquet as pq
from pipeline_import.configs import get_cfg


def load_chess_games(player: str,
                     perf_type: str,
                     data_date: date,
                     local_stockfish: bool,
                     io_dir: Path,
                     ) -> None:
    table_name = 'chess_games'
    id_cols = ['player', 'game_link']
    parquet_filename = 'game_infos'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def load_position_evals(player: str,
                        perf_type: str,
                        data_date: date,
                        local_stockfish: bool,
                        io_dir: Path,
                        ) -> None:
    table_name = 'position_evals'
    id_cols = ['fen']
    parquet_filename = 'evals'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def load_game_positions(player: str,
                        perf_type: str,
                        data_date: date,
                        local_stockfish: bool,
                        io_dir: Path,
                        ) -> None:
    table_name = 'game_positions'
    id_cols = ['game_link', 'half_move']
    parquet_filename = 'exploded_positions'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def load_game_materials(player: str,
                        perf_type: str,
                        data_date: date,
                        local_stockfish: bool,
                        io_dir: Path,
                        ) -> None:
    table_name = 'game_materials'
    id_cols = ['game_link', 'half_move']
    parquet_filename = 'exploded_materials'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def load_move_clocks(player: str,
                     perf_type: str,
                     data_date: date,
                     local_stockfish: bool,
                     io_dir: Path,
                     ) -> None:
    table_name = 'game_clocks'
    id_cols = ['game_link', 'half_move']
    parquet_filename = 'exploded_clocks'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def load_move_list(player: str,
                   perf_type: str,
                   data_date: date,
                   local_stockfish: bool,
                   io_dir: Path,
                   ) -> None:
    table_name = 'game_moves'
    id_cols = ['game_link', 'half_move']
    parquet_filename = 'exploded_moves'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def load_win_probs(player: str,
                   perf_type: str,
                   data_date: date,
                   local_stockfish: bool,
                   io_dir: Path,
                   ) -> None:
    table_name = 'win_probabilities'
    id_cols = ['game_link', 'half_move']
    parquet_filename = 'win_probabilities'

    _load_to_table(table_name=table_name,
                   parquet_filename=parquet_filename,
                   id_cols=id_cols,
                   io_dir=io_dir,
                   )


def _load_to_table(table_name: str,
                   parquet_filename: str,
                   id_cols: list[str],
                   io_dir: Path,
                   ) -> None:
    pg_cfg = get_cfg('postgres_cfg')

    uri = 'postgresql://{}:{}@{}:{}/{}'
    uri = uri.format(pg_cfg['user'],
                     pg_cfg['password'],
                     pg_cfg['host'],
                     pg_cfg['port'],
                     pg_cfg['database'],
                     )

    reader = pq.ParquetFile(io_dir / f'{parquet_filename}.parquet')

    temp_table_name = f'temp_{table_name}'

    insert_sql = f"""
        insert into {table_name} ({{columns}})
        select {{columns}} from {temp_table_name}
    """

    get_col_names = f"""
        select column_name from information_schema.columns
        where table_name = '{table_name}' and column_name != 'id'
    """

    drop_if_in_temp_table = f"""
        delete from {table_name} where ({', '.join(id_cols)}) in (
            select distinct ({', '.join(id_cols)}) from {temp_table_name}
        )
    """

    with adbc_driver_postgresql.dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            # todo: schema only read specific columns,
            # or insert new columns with warning?
            cur.execute(get_col_names)
            columns = [row[0] for row in cur.fetchall()]

            rows = cur.adbc_ingest(temp_table_name,
                                   reader.iter_batches(columns=columns),
                                   mode='create',
                                   temporary=True,
                                   )
            print(f'{rows=}')
            # handle upserting
            cur.execute(drop_if_in_temp_table)
            cur.execute(insert_sql.format(columns=', '.join(columns)))
            print('inserted')

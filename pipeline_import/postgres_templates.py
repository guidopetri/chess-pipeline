#! /usr/bin/env python3

from collections import OrderedDict
from luigi.contrib import postgres
from luigi.parameter import Parameter, ListParameter, DictParameter
from luigi.parameter import TaskParameter
from luigi import Task


class HashableDict(OrderedDict):
    def __hash__(self):
        return hash(frozenset(self))


class PostgresTable(postgres.CopyToTable):
    from .configs import postgres_cfg

    pg_cfg = postgres_cfg()
    user = pg_cfg.user
    password = pg_cfg.password
    host = pg_cfg.host
    port = pg_cfg.port
    database = pg_cfg.database


class TransactionFactTable(PostgresTable):
    """
    Copy a pandas DataFrame in a transaction fact table fashion to PostGreSQL.

    :param table: The table to write to.
    :param columns: The columns, in order they show up in the PostGreSQL table.
    :param fn: The Task that provides the data to load.
    :param id_cols: The columns to be used to identify whether a row is already
                    in the table.
    :param merge_cols: The columns representing dimension tables. In a
                       dictionary format, with the key being the `left` to
                       merge with, and the value being a tuple of
                       (`table`, `right`).
    """

    table = Parameter(default='')
    fn = TaskParameter(default=Task)
    columns = ListParameter(default=[])
    id_cols = ListParameter(default=[])
    merge_cols = DictParameter(default={})

    column_separator = '\t'

    def requires(self):
        return self.clone(self.fn)

    def rows(self):
        from pandas import read_pickle, DataFrame

        connection = self.output().connect()
        cursor = connection.cursor()

        sql = """SELECT %s FROM %s;""" % (', '.join(self.id_cols),
                                          self.table)

        cursor.execute(sql)
        results = cursor.fetchall()

        current_df = DataFrame(results, columns=self.id_cols)

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if not df.empty:
            # self.id_cols is tuple for some reason, so we need to convert
            # to list first
            # also, .isin compares by index as well as columns if passed a DF
            # to avoid that we can pass the dict with orient='list' and compare
            # just the columns
            df = df[~(df[list(self.id_cols)]
                      .isin(current_df.to_dict(orient='list'))
                      .all(axis=1))]
            df = df[list(self.columns)]

        for index, line in df.iterrows():  # returns (index, Series) tuple
            yield line.values.tolist()


class CopyWrapper(Task):
    """
    Dynamically require each Task in the `jobs` list and remove all local
    outputs.

    In order to write to multiple tables, we need to pass in a different
    `table` parameter each time. Using the `jobs` list, we can pass in a `dict`
    that has the appropriate parameters for each requirement.

    :param jobs: List of dictionaries. Each dictionary specifies one Task to
                 run and copy to a PostGreSQL table. Necessary keys are:

                 - `table`: The PostGreSQL table.
                 - `fn`: The Task to run in order to get the data.
                 - `columns: The table's columns, in the order they show up in
                             the PostGreSQL table.
                 - `id_cols`: The columns to be used to identify whether a row
                              is already present.
                 - `date_cols`: The columns that have to be converted to
                                datetime before copying.
                 - `merge_cols`: The columns that should represent dimension
                                 tables. This should be in the form of a
                                 `HashableDict()`, with the `left` column name
                                 as key, and a tuple of (`table`, `right`) as
                                 value.
    """

    jobs = []

    def requires(self):
        for job in self.jobs:
            vars(self).update(job)
            yield self.clone(job['table_type'])

    def run(self):
        import os
        import shutil

        filepath = os.path.expanduser('~/Temp/luigi')

        for file in os.listdir(filepath):
            full_path = os.path.join(filepath, file)
            if os.path.isfile(full_path):
                os.remove(full_path)
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)

    def complete(self):
        import os

        filepath = os.path.expanduser('~/Temp/luigi')

        if not os.path.exists(filepath):
            return False

        existing_files = os.listdir(filepath)
        finished_tasks = [inp.exists() for inp in self.input()]

        return (existing_files == []) and (any(finished_tasks))

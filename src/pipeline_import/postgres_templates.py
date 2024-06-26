#! /usr/bin/env python3

from collections import OrderedDict

from luigi import LocalTarget, Task
from luigi.contrib import postgres
from luigi.parameter import (
    DictParameter,
    ListParameter,
    Parameter,
    TaskParameter,
)
from luigi.task import flatten


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
        from pandas import read_pickle, read_sql_query

        connection = self.output().connect()

        sql = f"""SELECT {', '.join(self.id_cols)} FROM {self.table};"""

        current_df = read_sql_query(sql, connection)
        current_df.columns = self.id_cols

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
    _local_files = []

    def requires(self):
        for job in self.jobs:
            vars(self).update(job)
            yield self.clone(job['table_type'])

    @property
    def local_files(self):
        # if we haven't processed the local targets yet
        if not self._local_files:
            for job in self.jobs:
                vars(self).update(job)
                task = self.clone(job['fn'])
                targets = self.get_local_files(task)
                for target in targets:
                    if isinstance(target, LocalTarget):
                        self._local_files.append(target.path)
            # deduplicate items
            self._local_files = list(set(self._local_files))

        return self._local_files

    def get_local_files(self, task):
        # recursively gets local files from each task's requires()
        r = flatten(task.output())
        for dependency in flatten(task.requires()):
            r += self.get_local_files(dependency)
        return r

    def run(self):
        import os
        import shutil

        for local_file in self.local_files:
            if os.path.isfile(local_file):
                os.remove(local_file)
            elif os.path.isdir(local_file):
                shutil.rmtree(local_file)

    def complete(self):
        import os

        filepath = os.path.expanduser('~/Temp/luigi')

        if not os.path.exists(filepath):
            return False

        if any(os.path.isfile(local_file) for local_file in self.local_files):
            return False

        finished_tasks = [inp.exists() for inp in self.input()]

        return all(finished_tasks)

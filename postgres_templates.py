#! /usr/bin/env python3

from collections import OrderedDict
from luigi.contrib import postgres
from luigi.parameter import Parameter, ListParameter, DictParameter
from luigi.parameter import IntParameter, TaskParameter, ParameterVisibility
from luigi import Task


class HashableDict(OrderedDict):
    def __hash__(self):
        return hash(frozenset(self))


class TransactionFactTable(postgres.CopyToTable):
    """
    Copy a pandas DataFrame in a transaction fact table fashion to PostGreSQL.

    :param user: The username to use to authenticate with PostGreSQL.
    :param password: The password to use to authenticate with PostGreSQL.
    :param host: The hostname where the PostGreSQL server is located.
    :param port: The port that PostGreSQL is listening to.
    :param database: The database name to write to.
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

    user = Parameter(visibility=ParameterVisibility.PRIVATE,
                     significant=False)
    password = Parameter(visibility=ParameterVisibility.PRIVATE,
                         significant=False)
    host = Parameter(visibility=ParameterVisibility.PRIVATE,
                     significant=False)
    port = IntParameter(visibility=ParameterVisibility.PRIVATE,
                        significant=False)
    database = Parameter(visibility=ParameterVisibility.PRIVATE,
                         significant=False)
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
            df = df[~df[self.id_cols].isin(current_df).any(axis=1)]
            df = df[list(self.columns)]

        for index, line in df.iterrows():  # returns (index,Series) tuple
            yield line.values.tolist()


class CopyWrapper(Task):
    """
    Dynamically require each Task in the `jobs` list and remove all local
    outputs.

    In order to write to multiple tables, we need to pass in a different
    `table` parameter each time. Using the `jobs` list, we can pass in a `dict`
    that has the appropriate parameters for each requirement.

    :param password: The PostGreSQL password.
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

    password = Parameter(visibility=ParameterVisibility.PRIVATE,
                         significant=False)

    jobs = []

    def requires(self):
        for job in self.jobs:
            self.table = job['table']
            self.fn = job['fn']
            self.columns = job['columns']
            self.id_col = job['id_cols']
            self.date_cols = job['date_cols']
            self.merge_cols = job['merge_cols']
            yield self.clone(job['table_type'])

    def run(self):
        import os

        for file in os.listdir('~/Temp'):
            os.remove('~/Temp/{}'.format(file))

    def complete(self):
        import os

        return os.listdir('~/Temp') == []

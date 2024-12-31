#! /usr/bin/env python3

import os
import pickle

import pandas as pd
from luigi import LocalTarget, Task
from luigi.format import Nop
from luigi.parameter import ListParameter, Parameter
from luigi.util import requires
from pipeline_import.configs import get_cfg
from pipeline_import.transforms import (
    get_weekly_data,
)
from utils.newsletter import (
    create_newsletter,
    generate_elo_by_weekday_text,
    generate_win_ratio_by_color_text,
    send_newsletter,
)


class GetData(Task):

    player = Parameter()
    columns = ListParameter(default=[])

    def run(self):
        pg_cfg = get_cfg('postgres_cfg')
        df = get_weekly_data(pg_cfg, self.player)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)

    def output(self):
        file_location = f'~/Temp/luigi/week-data-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_location), format=Nop)


@requires(GetData)
class WinRatioByColor(Task):

    def output(self):
        file_loc = f'~/Temp/luigi/graphs/win-by-color-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)

    def run(self):
        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        text = generate_win_ratio_by_color_text(df, self.player)

        with self.output().open('w') as f:
            pickle.dump(text, f, protocol=-1)


@requires(GetData)
class EloByWeekday(Task):

    category = Parameter(default='blitz')

    def output(self):
        file_loc = f'~/Temp/luigi/graphs/elo-by-weekday-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)

    def run(self):
        with self.input().open('r') as f:
            df = pd.read_pickle(f, compression=None)

        text = generate_elo_by_weekday_text(df, self.category, self.player)

        with self.output().open('w') as f:
            pickle.dump(text, f, protocol=-1)


@requires(WinRatioByColor, EloByWeekday)
class CreateNewsletter(Task):

    receiver = Parameter()

    def run(self):
        newsletter = create_newsletter(inputs=self.input(),
                                       player=self.player,
                                       receiver=self.receiver,
                                       )

        with self.output().open('w') as f:
            pickle.dump(newsletter, f, protocol=-1)

    def output(self):
        file_loc = f'~/Temp/luigi/newsletter-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)


@requires(CreateNewsletter)
class SendNewsletter(Task):

    result = False

    def run(self):
        with self.input().open('r') as f:
            newsletter = pickle.load(f)

        self.result = send_newsletter(newsletter)

    def complete(self):
        return self.result

    def output(self):
        pass

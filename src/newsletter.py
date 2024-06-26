#! /usr/bin/env python3

from luigi import LocalTarget, Task
from luigi.format import Nop
from luigi.parameter import ListParameter, Parameter
from luigi.util import requires
from pipeline_import.configs import newsletter_cfg, postgres_cfg, sendgrid
from pipeline_import.newsletter_utils import get_color_stats_text
from pipeline_import.plots import (
    make_color_stats_plot,
    make_elo_by_weekday_plot,
)
from pipeline_import.transforms import (
    get_color_stats,
    get_elo_by_weekday,
    get_weekly_data,
)


class GetData(Task):

    player = Parameter()
    columns = ListParameter(default=[])

    def run(self):
        pg_cfg = postgres_cfg()
        df = get_weekly_data(pg_cfg, self.player)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)

    def output(self):
        import os

        file_location = f'~/Temp/luigi/week-data-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_location), format=Nop)


@requires(GetData)
class WinRatioByColor(Task):

    def output(self):
        import os

        file_loc = f'~/Temp/luigi/graphs/win-by-color-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)

    def run(self):
        import os
        import pickle

        from pandas import read_pickle

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:
            text = "Wait a second, no you didn't!"

            with self.output().open('w') as f:
                pickle.dump(text, f, protocol=-1)

            return

        color_stats = get_color_stats(df)

        fig_loc = '~/Temp/luigi/graphs'
        fig_loc = os.path.expanduser(fig_loc)
        filename = f'win-by-color-{self.player}.png'
        make_color_stats_plot(color_stats,
                              fig_loc=fig_loc,
                              filename=filename)

        win_rate_string = get_color_stats_text(color_stats)

        text = (win_rate_string
                + " <br> <img alt='Win rate by color "
                + "played' src='cid:win-by-color'><br>"
                )

        with self.output().open('w') as f:
            pickle.dump(text, f, protocol=-1)


@requires(GetData)
class EloByWeekday(Task):

    category = Parameter(default='blitz')

    def output(self):
        import os

        file_loc = f'~/Temp/luigi/graphs/elo-by-weekday-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)

    def run(self):
        import os
        import pickle

        from pandas import read_pickle

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:
            text = '\n'

            with self.output().open('w') as f:
                pickle.dump(text, f, protocol=-1)

            return

        elo = get_elo_by_weekday(df, category=self.category)

        fig_loc = '~/Temp/luigi/graphs'
        fig_loc = os.path.expanduser(fig_loc)
        filename = f'elo-by-weekday-{self.player}.png'
        make_elo_by_weekday_plot(elo, fig_loc=fig_loc, filename=filename)

        max_elo = int(elo['max'].max())
        min_elo = int(elo['min'].min())

        text = (f'This week, your highest elo in {self.category} was '
                f'{max_elo} and your lowest elo was {min_elo}. <br>'
                f"<img alt='Elo by weekday' src="
                f"'cid:elo-by-weekday'><br>"
                )

        with self.output().open('w') as f:
            pickle.dump(text, f, protocol=-1)


@requires(WinRatioByColor, EloByWeekday)
class CreateNewsletter(Task):

    receiver = Parameter()

    def run(self):
        import base64
        import os
        import pickle

        from bs4 import BeautifulSoup
        from sendgrid.helpers import mail

        newsletter = mail.Mail(from_email=newsletter_cfg().sender,
                               to_emails=self.receiver,
                               subject=f'Chess Newsletter - {self.player}',
                               )

        imgs_loc = os.path.expanduser('~/Temp/luigi/graphs/')

        for file in os.listdir(imgs_loc):
            if file.endswith('.png') and self.player in file:
                with open(os.path.join(imgs_loc, file), 'rb') as f:
                    encoded_img = base64.b64encode(f.read()).decode('utf-8')

                attachment = mail.Attachment(file_content=encoded_img,
                                             file_name=file,
                                             file_type='image/png',
                                             disposition='inline',
                                             content_id=file[:-4],
                                             )

                newsletter.add_attachment(attachment)

        message = [f'<html><body> Hi {self.player},<br><br>'
                   f"This week you played chess! Here's your performance:"
                   ]

        for inp in self.input():
            with inp.open('r') as f:
                text = pickle.load(f)
                message.append(text)

        message.append('Hope you do well this upcoming week!</body></html>')

        full_message = '<br>'.join(message)

        newsletter.add_content(full_message, 'text/html')

        # add plaintext MIME to make it less likely to be categorized as spam
        newsletter.add_content(BeautifulSoup(full_message,
                                             'html.parser').get_text(),
                               'text/plain')

        with self.output().open('w') as f:
            pickle.dump(newsletter, f, protocol=-1)

    def output(self):
        import os

        file_loc = f'~/Temp/luigi/newsletter-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)


@requires(CreateNewsletter)
class SendNewsletter(Task):

    result = False

    def run(self):
        import os
        import pickle
        import shutil

        from sendgrid import SendGridAPIClient

        with self.input().open('r') as f:
            newsletter = pickle.load(f)

        client = SendGridAPIClient(sendgrid().apikey)
        response = client.send(newsletter)

        self.result = response.status_code == 202

        filepath = os.path.expanduser('~/Temp/luigi')

        for file in os.listdir(filepath):
            full_path = os.path.join(filepath, file)
            if os.path.isfile(full_path):
                os.remove(full_path)
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)

    def complete(self):
        return self.result

    def output(self):
        pass

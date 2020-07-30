#! /usr/bin/env python3

from luigi import Task, LocalTarget
from luigi.format import Nop
from luigi.util import requires
from luigi.parameter import Parameter, ListParameter
from pipeline_import.configs import sendgrid, newsletter_cfg, postgres_cfg


class GetData(Task):

    player = Parameter()
    columns = ListParameter(default=[])

    def run(self):
        from psycopg2 import connect
        from pandas import DataFrame

        pg_cfg = postgres_cfg()
        db_connection_string = 'postgresql://{}:{}@{}:{}/{}'

        with connect(db_connection_string.format(pg_cfg.read_user,
                                                 pg_cfg.read_password,
                                                 pg_cfg.host,
                                                 pg_cfg.port,
                                                 pg_cfg.database)) as con:
            cursor = con.cursor()

            sql = f"""SELECT * from chess_games
                      WHERE player = '{self.player}'
                      AND datetime_played >= now()::date - interval '7 days';
                   """

            cursor.execute(sql)
            colnames = [desc.name for desc in cursor.description]
            results = cursor.fetchall()

        df = DataFrame.from_records(results, columns=colnames)

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
        import pickle
        import os
        from pandas import read_pickle
        from seaborn import set as sns_set
        from matplotlib import use

        use('Agg')

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:
            text = 'Wait a second, no you didn\'t!'

            with self.output().open('w') as f:
                pickle.dump(text, f, protocol=-1)

            return

        color_stats = df.groupby(['time_control_category',
                                  'player_color',
                                  'player_result']).agg({'id': 'nunique'})
        color_stats.reset_index(drop=False, inplace=True)

        # pivot so the columns are the player result
        color_stats = color_stats.pivot_table(index=['time_control_category',
                                                     'player_color'],
                                              columns='player_result',
                                              values='id')

        # divide each row by the sum of the row
        color_stats = color_stats.div(color_stats.sum(axis=1), axis=0)

        # set seaborn style for plots
        sns_set(style='whitegrid')

        # reorder columns
        for col in ['Win', 'Draw', 'Loss']:
            if col not in color_stats:
                color_stats[col] = 0
        color_stats = color_stats[['Win', 'Draw', 'Loss']]

        ax = color_stats.plot(kind='bar',
                              stacked=True,
                              color='gyr',
                              ylim=(0, 1),
                              rot=0,
                              title='Win-loss ratio by color played',
                              yticks=[0.0,
                                      0.1,
                                      0.2,
                                      0.3,
                                      0.4,
                                      0.5,
                                      0.6,
                                      0.7,
                                      0.8,
                                      0.9,
                                      1.0,
                                      1.01],  # enforce two digits of precision
                              )
        ax.set_ylabel('Ratio')
        ax.set_xlabel('Category / Color')
        ax.legend().set_title('')  # remove title

        for p in ax.patches:
            # place win% in the bar itself
            ax.annotate(f'{100 * p.get_height():.2f}%',
                        xy=(0.5, 0.5),
                        xycoords=p,
                        ha='center',
                        va='center',
                        )

        # save the figure
        fig_loc = '~/Temp/luigi/graphs'
        fig_loc = os.path.expanduser(fig_loc)
        filename = f'win-by-color-{self.player}.png'
        os.makedirs(fig_loc, exist_ok=True)
        ax.get_figure().savefig(os.path.join(fig_loc, filename),
                                bbox_inches='tight')

        text = ('You had a {:.2f}% win rate with {} in {}'
                ' and a {:.2f}% win rate with {}. <br>'
                '<img alt=\'Win rate by color played\' src='
                '\'cid:win-by-color\'><br>'
                .format(100 * color_stats.iloc[0][0],
                        *color_stats.iloc[0].name[::-1],
                        100 * color_stats.iloc[1][0],
                        color_stats.iloc[1].name[1],
                        ))

        with self.output().open('w') as f:
            pickle.dump(text, f, protocol=-1)


@requires(GetData)
class EloByWeekday(Task):

    def output(self):
        import os

        file_loc = f'~/Temp/luigi/graphs/elo-by-weekday-{self.player}.pckl'
        return LocalTarget(os.path.expanduser(file_loc), format=Nop)

    def run(self):
        import pickle
        import os
        from pandas import read_pickle
        from seaborn import set as sns_set
        from matplotlib import use

        use('Agg')

        with self.input().open('r') as f:
            df = read_pickle(f, compression=None)

        if df.empty:
            text = '\n'

            with self.output().open('w') as f:
                pickle.dump(text, f, protocol=-1)

            return

        df = df[df['time_control_category'] == 'blitz']
        df['weekday_played'] = df['datetime_played'].dt.weekday

        df['weekday_played'].replace(6, -1, inplace=True)
        df['weekday_played'] += 1  # what a dumb way of fixing this

        elo = (df.groupby('weekday_played')
                 .agg({'player_elo': ['mean',
                                      'std',
                                      'min',
                                      'max']}))
        # drop the first index on columns
        elo = (elo.T
                  .reset_index(level=0, drop=True)
                  .T
                  .reset_index(drop=False))
        elo.sort_values(by='weekday_played', inplace=True)

        sns_set(style='whitegrid')

        # plot main line with standard deviation
        ax = elo.plot(x='weekday_played',
                      y='mean',
                      yerr='std',
                      color='#0000FF',
                      title='Elo evolution by day of week',
                      style=[''],
                      legend=False,
                      capsize=4,
                      capthick=1,
                      )

        # plot min/maxes
        elo.plot(x='weekday_played',
                 y=['min', 'max'],
                 color='#999999',
                 style=['--', '--'],
                 ax=ax,
                 legend=False,
                 xlim=[-0.05, 6.05],
                 xticks=range(0, 7),
                 )

        min_last_day = elo[-1:]['min'].values
        max_last_day = elo[-1:]['max'].values
        mean_last_day = elo[-1:]['mean'].values

        # annotate the lines individually
        ax.annotate('min',
                    xy=(elo.shape[0] - 0.95, min_last_day),
                    color='#555555',
                    )
        ax.annotate('mean + std',
                    xy=(elo.shape[0] - 0.95, mean_last_day),
                    color='k',
                    )
        ax.annotate('max',
                    xy=(elo.shape[0] - 0.95, max_last_day),
                    color='#555555',
                    )

        # change the tick labels
        ax.set_xticklabels(['Sunday',
                            'Monday',
                            'Tuesday',
                            'Wednesday',
                            'Thursday',
                            'Friday',
                            'Saturday',
                            ],
                           rotation=45)

        ax.set_xlabel('Weekday')
        ax.set_ylabel('Rating')

        # save the figure
        fig_loc = '~/Temp/luigi/graphs'
        fig_loc = os.path.expanduser(fig_loc)
        filename = f'elo-by-weekday-{self.player}.png'
        os.makedirs(fig_loc, exist_ok=True)
        ax.get_figure().savefig(os.path.join(fig_loc, filename),
                                bbox_inches='tight')

        max_elo = int(elo['max'].max())
        min_elo = int(elo['min'].min())

        text = (f'This week, your highest elo in blitz was {max_elo} and'
                f' your lowest elo was {min_elo}. <br>'
                f'<img alt=\'Elo by weekday\' src='
                f'\'cid:elo-by-weekday\'><br>'
                )

        with self.output().open('w') as f:
            pickle.dump(text, f, protocol=-1)


@requires(WinRatioByColor, EloByWeekday)
class CreateNewsletter(Task):

    receiver = Parameter()

    def run(self):
        import pickle
        import base64
        import os
        from sendgrid.helpers import mail
        from bs4 import BeautifulSoup

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
                   f'This week you played chess! Here\'s your performance:'
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
        import shutil
        import pickle
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

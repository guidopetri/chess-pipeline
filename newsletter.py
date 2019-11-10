#! /usr/bin/env python3

from luigi import Task, LocalTarget, WrapperTask
from luigi.format import Nop
from luigi.util import requires, inherits
from luigi.parameter import Parameter, ListParameter
from configs import sendgrid, newsletter_cfg, postgres_cfg


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

            sql = """SELECT {} from chess_games
                     WHERE player = {}
                     AND datetime_played >= now()::date - interval '7 days';
                  """

            cursor.execute(sql.format((', '.join(self.columns),
                                       self.player)))
            colnames = [desc.name for desc in cursor.description]
            results = cursor.fetchall()

        df = DataFrame.from_records(results, columns=colnames)

        with self.output().temporary_path() as temp_output_path:
            df.to_pickle(temp_output_path, compression=None)

    def output(self):
        import os

        file_location = '~/Temp/luigi/week-data-{}.pckl'.format(self.player)
        return LocalTarget(os.path.expanduser(file_location), format=Nop)


@inherits(GetData)
class CreateGraphs(WrapperTask):

    def requires(self):
        yield 'tasks that write their own bits of email here'


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
                               subject=('Chess Newsletter - {}'
                                        .format(self.player)),
                               )

        imgs_loc = os.path.expanduser('~/Temp/luigi/graphs/')

        for file in os.listdir(imgs_loc):
            if file.endswith('.png'):
                with open(os.path.join(imgs_loc, file), 'rb') as f:
                    encoded_img = base64.b64encode(f.read()).decode('utf-8')

                attachment = mail.Attachment(file_content=encoded_img,
                                             file_name=file,
                                             file_type='image/png',
                                             disposition='inline',
                                             content_id=file[:-4],
                                             )

                newsletter.add_attachment(attachment)

        message = ['<html><body> Hi {},<br><br>'
                   'This week you played chess! Here\'s your performance:'
                   .format(self.player)
                   ]

        # for inp in self.input():
        #     with inp.open('r') as f:
        #         text = pickle.load(f)
        #         message.append(text)

        with self.input().open('r') as f:
            text = pickle.load(f)
            message.append(text)

        message.append('</body></html>')

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

        file_location = '~/Temp/luigi/newsletter.pckl'
        return LocalTarget(os.path.expanduser(file_location), format=Nop)


@requires(CreateNewsletter)
class SendNewsletter(Task):

    result = False

    def run(self):
        import os
        import pickle
        from sendgrid import SendGridAPIClient

        with self.input().open('r') as f:
            newsletter = pickle.load(f)

        client = SendGridAPIClient(sendgrid().apikey)
        response = client.send(newsletter)

        self.result = response.status_code == 202

        filepath = os.path.expanduser('~/Temp/luigi')

        for file in os.listdir(filepath):
            os.remove(filepath + '/{}'.format(file))

    def complete(self):
        return self.result

    def output(self):
        pass

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

        with connect(db_connection_string.format(pg_cfg.user,
                                                 pg_cfg.password,
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
        from sendgrid.helpers import mail
        from bs4 import BeautifulSoup

        newsletter = mail.Mail(from_email=newsletter_cfg().sender,
                               to_emails=self.receiver,
                               subject=('Chess Newsletter - {}'
                                        .format(self.player)),
                               )

        img_loc = ''
        with open(img_loc, 'rb') as f:
            encoded_img = base64.b64encode(f.read()).decode('utf-8')

        encoded_img_cid = 'classical'
        attachment = mail.Attachment(file_content=encoded_img,
                                     file_name='classical-coast.jpg',
                                     file_type='image/jpeg',
                                     disposition='inline',
                                     content_id=encoded_img_cid
                                     )

        newsletter.add_attachment(attachment)

        message = ("""<html><body><p>this is a test. maybe i need to add a
         few more words
                    so that my spam filter doesn't pick up on this.
                    i need about
                    400-800 b ytes of words so maybe lorem ipsum this is a
                    test
                    The problem with CID embedded images is that they don’t
                     always display properly in email clients. The rule of
                     thumb I use is that CID embedding will work fine in the
                     majority of desktop email clients, but most likely not at
                     all in web based email clients such as Gmail, or Yahoo!
                     Mail. Bummer.
                    The problem with CID embedded images is that they don’t
                    always display properly in email clients. The rule of
                    thumb I use is that CID embedding will work fine in the
                    majority of desktop email clients, but most likely not at
                    all in web based email clients such as Gmail, or Yahoo!
                    Mail. Bummer.
                    The problem with CID embedded images is that they don’t
                    always display properly in email clients. The rule of
                    thumb I use is that CID embedding will work fine in the
                    majority of desktop email clients, but most likely not at
                    all in web based email clients such as Gmail, or Yahoo!
                    Mail. Bummer.
                    The problem with CID embedded images is that they don’t
                    always display properly in email clients. The rule of
                    thumb I use is that CID embedding will work fine in the
                    majority of desktop email clients, but most likely not at
                    all in web based email clients such as Gmail, or Yahoo!
                    Mail. Bummer.</p>
                   <img alt='Classical Coast'
                   src='cid:{}'>
                   </body></html>""".format(encoded_img_cid))

        newsletter.add_content(message, 'text/html')

        # add plaintext MIME to make it less likely to be categorized as spam
        newsletter.add_content(BeautifulSoup(message,
                                             parser='html.parser').get_text(),
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
        print(response.status_code)
        print(response.body, response.headers)
        self.result = response.status_code == 202

        filepath = os.path.expanduser('~/Temp/luigi')

        for file in os.listdir(filepath):
            os.remove(filepath + '/{}'.format(file))

    def complete(self):
        return self.result

    def output(self):
        pass

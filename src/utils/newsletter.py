import base64
import os
import pickle
import shutil

from bs4 import BeautifulSoup
from pipeline_import.configs import newsletter_cfg, sendgrid
from pipeline_import.newsletter_utils import get_color_stats_text
from pipeline_import.plots import (
    make_color_stats_plot,
    make_elo_by_weekday_plot,
)
from pipeline_import.transforms import (
    get_color_stats,
    get_elo_by_weekday,
)
from sendgrid import SendGridAPIClient
from sendgrid.helpers import mail


def generate_elo_by_weekday_text(df, category, player):
    if df.empty:
        return '\n'

    elo = get_elo_by_weekday(df, category=category)

    fig_loc = '~/Temp/luigi/graphs'
    fig_loc = os.path.expanduser(fig_loc)
    filename = f'elo-by-weekday-{player}.png'
    make_elo_by_weekday_plot(elo, fig_loc=fig_loc, filename=filename)

    max_elo = int(elo['max'].max())
    min_elo = int(elo['min'].min())

    text = (f'This week, your highest elo in {category} was '
            f'{max_elo} and your lowest elo was {min_elo}. <br>'
            f"<img alt='Elo by weekday' src="
            f"'cid:elo-by-weekday'><br>"
            )
    return text


def generate_win_ratio_by_color_text(df, player):
    if df.empty:
        return "Wait a second, no you didn't!"

    color_stats = get_color_stats(df)

    fig_loc = '~/Temp/luigi/graphs'
    fig_loc = os.path.expanduser(fig_loc)
    filename = f'win-by-color-{player}.png'
    make_color_stats_plot(color_stats,
                          fig_loc=fig_loc,
                          filename=filename)

    win_rate_string = get_color_stats_text(color_stats)

    text = (win_rate_string
            + " <br> <img alt='Win rate by color "
            + "played' src='cid:win-by-color'><br>"
            )
    return text


def send_newsletter(newsletter) -> bool:
    client = SendGridAPIClient(sendgrid().apikey)
    response = client.send(newsletter)

    filepath = os.path.expanduser('~/Temp/luigi')

    for file in os.listdir(filepath):
        full_path = os.path.join(filepath, file)
        if os.path.isfile(full_path):
            os.remove(full_path)
        elif os.path.isdir(full_path):
            shutil.rmtree(full_path)
    return response.status_code == 202


def create_newsletter(inputs, player, receiver):
    newsletter = mail.Mail(from_email=newsletter_cfg().sender,
                           to_emails=receiver,
                           subject=f'Chess Newsletter - {player}',
                           )

    imgs_loc = os.path.expanduser('~/Temp/luigi/graphs/')

    for file in os.listdir(imgs_loc):
        if file.endswith('.png') and player in file:
            with open(os.path.join(imgs_loc, file), 'rb') as f:
                encoded_img = base64.b64encode(f.read()).decode('utf-8')

            attachment = mail.Attachment(file_content=encoded_img,
                                         file_name=file,
                                         file_type='image/png',
                                         disposition='inline',
                                         content_id=file[:-4],
                                         )

            newsletter.add_attachment(attachment)

    message = [f'<html><body> Hi {player},<br><br>'
               f"This week you played chess! Here's your performance:"
               ]

    for inp in inputs:
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
    return newsletter

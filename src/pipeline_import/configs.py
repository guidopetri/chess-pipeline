#! /usr/bin/env python3

from luigi import Config
from luigi.parameter import ParameterVisibility, IntParameter, Parameter


class sendgrid(Config):
    apikey = Parameter(config_path={'section': 'email',
                                    'name': 'SENGRID_API_KEY'},
                       description='API key for SendGrid login')


class newsletter_cfg(Config):
    sender = Parameter()


class postgres_cfg(Config):
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
    read_user = Parameter(visibility=ParameterVisibility.PRIVATE,
                          significant=False)
    read_password = Parameter(visibility=ParameterVisibility.PRIVATE,
                              significant=False)


class lichess_token(Config):
    token = Parameter(visibility=ParameterVisibility.PRIVATE,
                      significant=False)


class stockfish_cfg(Config):
    depth = IntParameter()
    location = Parameter(visibility=ParameterVisibility.PRIVATE,
                         significant=False)

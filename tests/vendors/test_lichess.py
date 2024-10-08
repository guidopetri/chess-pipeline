from collections import defaultdict
from datetime import datetime

import freezegun
import pytest
from lichess.format import JSON, PYCHESS
from vendors.lichess import fetch_lichess_api_json, fetch_lichess_api_pgn


@pytest.fixture
def mock_lichess_api_json(mocker):
    # taken from lichess api examples
    sample_json = [{'id': 'q7ZvsdUF',
                    'rated': True,
                    'variant': 'standard',
                    'speed': 'blitz',
                    'perf': 'blitz',
                    'createdAt': 1514505150384,
                    'lastMoveAt': 1514505592843,
                    'status': 'draw',
                    'players': {
                      'white': {
                        'user': {
                          'name': 'Lance5500',
                          'title': 'LM',
                          'patron': True,
                          'id': 'lance5500'
                        },
                        'rating': 2389,
                        'ratingDiff': 4
                      },
                      'black': {
                        'user': {
                          'name': 'TryingHard87',
                          'id': 'tryinghard87'
                        },
                        'rating': 2498,
                        'ratingDiff': -4
                      }
                    },
                    'opening': {
                      'eco': 'D31',
                      'name': 'Semi-Slav Defense: Marshall Gambit',
                      'ply': 7
                    },
                    'moves': 'd4 d5 c4 c6 Nc3 e6',
                    'clock': {
                      'initial': 300,
                      'increment': 3,
                      'totalTime': 420
                    }
                    }]
    mock_lichess_api = mocker.patch('lichess.api.user_games',
                                    return_value=sample_json)
    return mock_lichess_api


@pytest.fixture
def mock_lichess_api_pgn(mocker):
    mock_lichess_api = mocker.patch('lichess.api.user_games',
                                    return_value=[None],
                                    )
    return mock_lichess_api


@pytest.fixture
def mock_parse_headers(mocker):
    mocker.patch('vendors.lichess.parse_headers',
                 return_value=defaultdict(str),
                 )


def test_lichess_api_json_single_day(mocker, mock_lichess_api_json, snapshot):
    player = 'thibault'
    perf_type = 'bullet'
    since = datetime(2024, 4, 28)
    single_day = True
    # converted manually to ms format
    since_unix = 1714262400000
    until = 1714348800000

    df = fetch_lichess_api_json(player=player,
                                perf_type=perf_type,
                                since=since,
                                single_day=single_day,
                                )

    mock_lichess_api_json.assert_called_once_with(player,
                                                  since=since_unix,
                                                  until=until,
                                                  perfType=perf_type,
                                                  auth=mocker.ANY,
                                                  evals='false',
                                                  clocks='false',
                                                  moves='false',
                                                  format=JSON,
                                                  )

    assert df.to_json() == snapshot


@freezegun.freeze_time('2024-04-30 00:00:00')
def test_lichess_api_json_multiple_day(mocker, mock_lichess_api_json):
    player = 'thibault'
    perf_type = 'bullet'
    since = datetime(2024, 4, 28)
    single_day = False
    # converted manually to ms format
    since_unix = 1714262400000
    until = 1714435200000

    _ = fetch_lichess_api_json(player=player,
                               perf_type=perf_type,
                               since=since,
                               single_day=single_day,
                               )

    mock_lichess_api_json.assert_called_once_with(player,
                                                  since=since_unix,
                                                  until=until,
                                                  perfType=perf_type,
                                                  auth=mocker.ANY,
                                                  evals='false',
                                                  clocks='false',
                                                  moves='false',
                                                  format=JSON,
                                                  )


def test_lichess_api_pgn_single_day(mocker,
                                    mock_lichess_api_pgn,
                                    mock_parse_headers,
                                    mock_task):
    player = 'thibault'
    perf_type = 'bullet'
    since = datetime(2024, 4, 28)
    single_day = True
    # converted manually to ms format
    since_unix = 1714262400000
    until = 1714348800000

    _ = fetch_lichess_api_pgn(player=player,
                              perf_type=perf_type,
                              since=since,
                              single_day=single_day,
                              game_count=1,
                              task=mock_task,
                              )

    mock_lichess_api_pgn.assert_called_once_with(player,
                                                 since=since_unix,
                                                 until=until,
                                                 perfType=perf_type,
                                                 auth=mocker.ANY,
                                                 evals='true',
                                                 clocks='true',
                                                 opening='true',
                                                 format=PYCHESS,
                                                 )


@freezegun.freeze_time('2024-04-30 00:00:00')
def test_lichess_api_pgn_multiple_day(mocker,
                                      mock_lichess_api_pgn,
                                      mock_parse_headers,
                                      mock_task):
    player = 'thibault'
    perf_type = 'bullet'
    since = datetime(2024, 4, 28)
    single_day = False
    # converted manually to ms format
    since_unix = 1714262400000
    until = 1714435200000

    _ = fetch_lichess_api_pgn(player=player,
                              perf_type=perf_type,
                              since=since,
                              single_day=single_day,
                              game_count=1,
                              task=mock_task,
                              )

    mock_lichess_api_pgn.assert_called_once_with(player,
                                                 since=since_unix,
                                                 until=until,
                                                 perfType=perf_type,
                                                 auth=mocker.ANY,
                                                 evals='true',
                                                 clocks='true',
                                                 opening='true',
                                                 format=PYCHESS,
                                                 )

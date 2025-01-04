from collections import defaultdict
from datetime import date

import pandas as pd
import pytest
from lichess.format import JSON, PYCHESS
from utils.output import get_output_file_prefix
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


@pytest.fixture
def mock_lichess_cfg(mocker):
    mocker.patch('vendors.lichess.get_cfg', return_value={'token': 'abc'})


def test_lichess_api_json(mock_lichess_api_json,
                          mock_lichess_cfg,
                          snapshot,
                          tmp_path,
                          ):
    player = 'thibault'
    perf_type = 'bullet'
    data_date = date(2024, 4, 28)
    # converted manually to ms format
    since_unix = 1714262400000
    until = 1714348800000
    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )

    fetch_lichess_api_json(player=player,
                           perf_type=perf_type,
                           data_date=data_date,
                           local_stockfish=True,
                           io_dir=tmp_path,
                           )

    mock_lichess_api_json.assert_called_once_with(player,
                                                  since=since_unix,
                                                  until=until,
                                                  perfType=perf_type,
                                                  auth='abc',
                                                  evals='false',
                                                  clocks='false',
                                                  moves='false',
                                                  format=JSON,
                                                  )

    df = pd.read_parquet(tmp_path / f'{prefix}_raw_json.parquet')
    assert df.to_json() == snapshot


def test_lichess_api_pgn(mock_lichess_api_pgn,
                         mock_parse_headers,
                         mock_lichess_cfg,
                         tmp_path,
                         ):
    player = 'thibault'
    perf_type = 'bullet'
    data_date = date(2024, 4, 28)
    # converted manually to ms format
    since_unix = 1714262400000
    until = 1714348800000

    prefix: str = get_output_file_prefix(player=player,
                                         perf_type=perf_type,
                                         data_date=data_date,
                                         )
    json_df = pd.DataFrame([['test']], columns=['abc'])
    json_df.to_parquet(tmp_path / f'{prefix}_raw_json.parquet')

    fetch_lichess_api_pgn(player=player,
                          perf_type=perf_type,
                          data_date=data_date,
                          local_stockfish=True,
                          io_dir=tmp_path,
                          )

    mock_lichess_api_pgn.assert_called_once_with(player,
                                                 since=since_unix,
                                                 until=until,
                                                 perfType=perf_type,
                                                 auth='abc',
                                                 evals='true',
                                                 clocks='true',
                                                 opening='true',
                                                 format=PYCHESS,
                                                 )

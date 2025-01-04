from datetime import date

import pandas as pd
import pytest
from utils.output import get_output_file_prefix
from vendors.stockfish import get_evals


@pytest.fixture
def mock_run_remote_sql_query(mocker):
    mocker.patch('vendors.stockfish.run_remote_sql_query',
                 return_value=pd.DataFrame([], columns=['fen']),
                 )


@pytest.fixture
def mock_stockfish_cfg(mocker):
    mocker.patch('vendors.stockfish.get_cfg',
                 return_value={'location': 'abc', 'depth': 1})


@pytest.fixture
def mock_stockfish(mocker):
    mocker.patch('vendors.stockfish.get_sf_evaluation', return_value=-9999)


def test_get_evals_on_checkmate_position(mocker,
                                         monkeypatch,
                                         tmp_path,
                                         mock_stockfish,
                                         mock_stockfish_cfg,
                                         mock_run_remote_sql_query,
                                         mocked_cloud_eval,
                                         ):
    # TODO: what is this test checking, exactly?
    mocker.patch('vendors.stockfish.valkey')
    monkeypatch.setenv('VALKEY_CONNECTION_URL', '')
    fen = 'rnb1k1nr/pp1p1ppp/4p3/8/8/1P2qN2/PBPKPbPP/RN1Q1B1R w kq - 2 7'

    prefix: str = get_output_file_prefix(player='test',
                                         perf_type='bullet',
                                         data_date=date(2025, 1, 1),
                                         )

    df = pd.DataFrame([[[], [], fen]],
                      columns=['evaluations', 'eval_depths', 'positions'],
                      )
    df.to_parquet(tmp_path / f'{prefix}_cleaned_df.parquet')
    get_evals(player='test',
              perf_type='bullet',
              data_date=date(2025, 1, 1),
              local_stockfish=True,
              io_dir=tmp_path,
              )
    actual = pd.read_parquet(tmp_path / f'{prefix}_evals.parquet')

    expected = pd.DataFrame([[fen[:-2], -9999, 1]],
                            columns=['fen', 'evaluation', 'eval_depth'])

    pd.testing.assert_frame_equal(actual, expected)

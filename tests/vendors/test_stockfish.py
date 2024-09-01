import pandas as pd
import pytest
from vendors.stockfish import get_evals


@pytest.fixture
def mock_run_remote_sql_query(mocker):
    mocker.patch('vendors.stockfish.run_remote_sql_query',
                 return_value=pd.DataFrame([], columns=['fen']),
                 )


def test_get_evals_on_checkmate_position(mock_run_remote_sql_query,
                                         mock_task,
                                         mocked_cloud_eval):
    fen = 'rnb1k1nr/pp1p1ppp/4p3/8/8/1P2qN2/PBPKPbPP/RN1Q1B1R w kq - 2 7'

    df = pd.DataFrame([[None, None, fen]],
                      columns=['evaluations', 'eval_depths', 'positions'],
                      )
    actual = get_evals(df, local_stockfish=True, task=mock_task)

    expected = pd.DataFrame([[fen[:-2], -9999, 1]],
                            columns=['fen', 'evaluation', 'eval_depth'])
    expected['eval_depth'] = expected['eval_depth'].astype(object)

    pd.testing.assert_frame_equal(actual, expected)

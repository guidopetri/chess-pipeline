import lichess.api
import pytest


@pytest.fixture
def mock_task(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mocked_cloud_eval(mocker):
    mocker.patch('lichess.api.cloud_eval',
                 side_effect=lichess.api.ApiHttpError(http_status=429,
                                                      url='https://link.com',
                                                      response_text='error',
                                                      )
                 )

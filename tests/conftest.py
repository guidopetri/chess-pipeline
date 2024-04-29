import pytest


@pytest.fixture
def mock_task(mocker):
    return mocker.MagicMock()

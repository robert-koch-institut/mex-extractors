from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture  # needed for hardcoded upload to S3.
def mocked_boto() -> Generator[MagicMock, None, None]:
    """Mock a S3 session client to write the jsons to."""
    with patch("boto3.Session") as mock_session_class:
        mock_s3_client = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session_instance

        yield mock_s3_client

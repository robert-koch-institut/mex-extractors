from unittest.mock import MagicMock

import pytest

from mex.extractors.pipeline import run_job_in_process
from mex.extractors.sinks.s3 import S3Sink


@pytest.fixture  # needed for hardcoded upload to S3.
def mocked_boto(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mocked_client = MagicMock()
    monkeypatch.setattr(
        S3Sink, "__init__", lambda self: setattr(self, "client", mocked_client)
    )
    return mocked_client


@pytest.mark.usefixtures("mocked_backend", "mocked_boto")
def test_run() -> None:
    assert run_job_in_process("publisher")

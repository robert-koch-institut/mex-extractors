import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_backend_api_connector", "mocked_provider", "mocked_boto"
)
def test_run() -> None:
    assert run_job_in_process("datenkompass")

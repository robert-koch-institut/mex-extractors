import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures("mocked_backend", "mocked_boto")
def test_run() -> None:
    result = run_job_in_process("publisher")
    assert result.success

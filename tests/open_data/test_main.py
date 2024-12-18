import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures("mocked_open_data")
def test_job() -> None:
    result = run_job_in_process("open_data")
    assert result.success

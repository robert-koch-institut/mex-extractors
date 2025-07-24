import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures("mocked_backend")
def test_job() -> None:
    assert run_job_in_process("consent_mailer")

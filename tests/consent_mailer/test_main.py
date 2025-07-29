import pytest
from mex.extractors.pipeline import run_job_in_process

@pytest.mark.usefixtures(
    "mocked_backend_api_connector"
)
def test_job() -> None:
    assert run_job_in_process("consent_mailer")

import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures("mocked_igs", "mocked_ldap")
def test_job() -> None:
    assert run_job_in_process("igs")

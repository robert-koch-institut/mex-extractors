import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_open_data",
    "mocked_wikidata",
    "mocked_ldap",
)
def test_job() -> None:
    assert run_job_in_process("open_data")

import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_ldap",
    "mocked_wikidata",
)
def test_job() -> None:
    result = run_job_in_process("sumo")
    assert result.success

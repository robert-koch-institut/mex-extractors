import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_grippeweb", "mocked_ldap", "mocked_wikidata", "mocked_wikidata_locations"
)
def test_job() -> None:
    assert run_job_in_process("grippeweb")

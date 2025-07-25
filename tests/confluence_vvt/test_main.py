import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_confluence_vvt",
    "mocked_ldap",
    "mocked_wikidata",
)
def test_job() -> None:
    assert run_job_in_process("confluence_vvt")

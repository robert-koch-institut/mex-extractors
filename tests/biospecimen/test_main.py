import pytest

from mex.extractors.pipeline.base import run_job_in_process


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_job() -> None:
    result = run_job_in_process("biospecimen")
    assert result.success

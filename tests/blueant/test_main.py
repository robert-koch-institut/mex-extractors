import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_ldap",
    "mocked_blueant",
    "mocked_wikidata",
)
def test_job() -> None:
    assert run_job_in_process("blueant")

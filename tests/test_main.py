import pytest

from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures(
    "mocked_blueant",
    "mocked_confluence_vvt",
    "mocked_datscha_web",
    "mocked_drop",
    "mocked_grippeweb",
    "mocked_ifsg",
    "mocked_igs",
    "mocked_kvis",
    "mocked_ldap",
    "mocked_open_data",
    "mocked_wikidata",
)
def test_job() -> None:
    assert run_job_in_process("all_extractors")

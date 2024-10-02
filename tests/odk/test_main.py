import pytest

from mex.common.models import ExtractedPrimarySource
from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_job(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    result = run_job_in_process("odk")
    assert result.success

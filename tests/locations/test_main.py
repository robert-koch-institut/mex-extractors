import pytest

from mex.extractors.locations.main import locations_wikidata_ids
from mex.extractors.pipeline import run_job_in_process


@pytest.mark.usefixtures("mocked_wikidata", "mocked_wikidata_locations")
def test_locations_wikidata_ids() -> None:
    """Verify IDs from raw data (Q183, Q945) + mocked SPARQL (Q183, Q42) are fetched."""
    result = locations_wikidata_ids()
    assert result == {"Q183", "Q42", "Q954"}


@pytest.mark.usefixtures("mocked_wikidata", "mocked_wikidata_locations")
def test_job() -> None:
    assert run_job_in_process("locations")

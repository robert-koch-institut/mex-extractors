import pytest

from mex.extractors.locations.extract import (
    extract_country_wikidata_ids_from_eu_vocabularies_json,
    extract_region_wikidata_ids,
)


def test_extract_country_wikidata_ids_from_eu_vocabularies_json() -> None:
    assert extract_country_wikidata_ids_from_eu_vocabularies_json() == ["Q183", "Q954"]


@pytest.mark.usefixtures("mocked_wikidata_locations")
def test_extract_region_wikidata_ids() -> None:
    assert extract_region_wikidata_ids() == ["Q183", "Q42"]

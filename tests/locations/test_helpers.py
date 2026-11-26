import pytest

from mex.common.models import ExtractedLocation
from mex.common.types import Text, TextLanguage
from mex.extractors.locations.helpers import get_location_merged_id_by_wikidata_id
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


@pytest.mark.usefixtures("mocked_wikidata", "mocked_wikidata_locations")
@pytest.mark.parametrize("wikidata_id", ["Q183", "http://www.wikidata.org/entity/Q183"])
def test_get_location_merged_id_by_wikidata_id(wikidata_id: str) -> None:
    expected = ExtractedLocation(
        hadPrimarySource=get_extracted_primary_source_id_by_name("wikidata"),
        identifierInPrimarySource="Q183",
        name=[Text(value="Ort Q183", language=TextLanguage.DE)],
    )
    assert get_location_merged_id_by_wikidata_id(wikidata_id) == (
        expected.stableTargetId
    )

from typing import TYPE_CHECKING

from mex.common.wikidata.extract import get_wikidata_location
from mex.common.wikidata.transform import (
    transform_wikidata_locations_to_extracted_locations,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedLocation


def transform_wikidata_ids_to_extracted_locations(
    locations_wikidata_ids: set[str],
) -> list[ExtractedLocation]:
    """Transform wikidata ids to extracted locations, using the wikidata helpers.

    Args:
        locations_wikidata_ids: list of wikidata ids

    Returns:
        list of extracted locations
    """
    wikidata_primary_source_id = get_extracted_primary_source_id_by_name("wikidata")

    wikidata_locations = [
        get_wikidata_location(wikidata_id) for wikidata_id in locations_wikidata_ids
    ]

    return list(
        transform_wikidata_locations_to_extracted_locations(
            wikidata_locations,
            wikidata_primary_source_id,
        )
    )

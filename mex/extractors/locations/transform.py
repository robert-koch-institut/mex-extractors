from typing import TYPE_CHECKING

from mex.common.wikidata.transform import (
    transform_wikidata_locations_to_extracted_locations,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedLocation
    from mex.common.types import MergedPrimarySourceIdentifier
    from mex.common.wikidata.models import WikidataLocation


def transform_wikidata_ids_to_extracted_locations(
    wikidata_locations: list[WikidataLocation],
    wikidata_primary_source_id: MergedPrimarySourceIdentifier,
) -> list[ExtractedLocation]:
    """Transform wikidata ids to extracted locations, using the wikidata helpers.

    Args:
        wikidata_locations: list of wikidata locations
        wikidata_primary_source_id: Identifier of merged primary source 'wikidata'git

    Returns:
        list of extracted locations
    """
    return list(
        transform_wikidata_locations_to_extracted_locations(
            wikidata_locations,
            wikidata_primary_source_id,
        )
    )

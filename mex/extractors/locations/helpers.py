from functools import lru_cache
from typing import TYPE_CHECKING

from mex.common.wikidata.extract import get_wikidata_location
from mex.common.wikidata.transform import (
    transform_wikidata_location_to_extracted_location,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from mex.common.types import MergedLocationIdentifier


@lru_cache(maxsize=1024)
def get_location_merged_id_by_wikidata_id(
    wikidata_id: str,
) -> MergedLocationIdentifier | None:
    """Get and load a wikidata location by its ID and return its stableTargetId.

    Args:
        wikidata_id: wikidata QID (e.g. `Q183`) or entity URL

    Returns:
        merged location identifier, or None if wikidata has no label for it
    """
    wikidata_location = get_wikidata_location(wikidata_id)
    extracted_location = transform_wikidata_location_to_extracted_location(
        wikidata_location, get_extracted_primary_source_id_by_name("wikidata")
    )
    if extracted_location:
        load([extracted_location])
        return extracted_location.stableTargetId
    return None

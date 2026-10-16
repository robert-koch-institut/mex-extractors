from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from mex.common.wikidata.extract import get_wikidata_location
from mex.common.wikidata.transform import (
    transform_wikidata_locations_to_extracted_locations,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from mex.common.models import ExtractedLocation
    from mex.common.types import MergedPrimarySourceIdentifier


def transform_wikidata_ids_to_extracted_locations(
    wikidata_ids: Iterable[str],
    primary_source_id: MergedPrimarySourceIdentifier,
) -> Iterator[ExtractedLocation]:
    """Transform wikidata ids to extracted locations."""
    wikidata_locations = (
        get_wikidata_location(wikidata_id) for wikidata_id in wikidata_ids
    )

    yield from transform_wikidata_locations_to_extracted_locations(
        wikidata_locations,
        primary_source_id,
    )

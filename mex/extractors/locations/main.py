from dagster import asset

from mex.common.models import ExtractedLocation
from mex.extractors.locations.extract import (
    extract_country_wikidata_ids_from_eu_vocabularies_json,
    extract_region_wikidata_ids,
)
from mex.extractors.locations.transform import (
    transform_wikidata_ids_to_extracted_locations,
)
from mex.extractors.sinks import load


@asset(group_name="locations")
def locations_wikidata_ids() -> set[str]:
    """Extract country wikidata IDs from json list."""
    return set(
        extract_country_wikidata_ids_from_eu_vocabularies_json()
        + extract_region_wikidata_ids()
    )


@asset(group_name="locations")
def locations_transform_wikidata_ids_to_extracted_locations(
    locations_wikidata_ids: set[str],
) -> list[ExtractedLocation]:
    """Extract countries from json list."""
    extracted_locations = transform_wikidata_ids_to_extracted_locations(
        locations_wikidata_ids
    )

    load(extracted_locations)
    return extracted_locations

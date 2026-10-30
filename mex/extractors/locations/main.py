from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import ExtractedLocation
from mex.common.wikidata.transform import (
    transform_wikidata_locations_to_extracted_locations,
)
from mex.extractors.locations.extract import (
    extract_country_wikidata_ids_from_eu_vocabularies_json,
    extract_region_wikidata_ids,
    extract_wikidata_locations,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
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
    """Extract, transform, and load Wikidata locations."""
    wikidata_primary_source_id = get_extracted_primary_source_id_by_name("wikidata")

    wikidata_locations = extract_wikidata_locations(sorted(locations_wikidata_ids))

    extracted_locations = list(
        transform_wikidata_locations_to_extracted_locations(
            wikidata_locations,
            wikidata_primary_source_id,
        )
    )

    load(extracted_locations)
    return extracted_locations


@entrypoint()
def run() -> None:  # pragma: no cover
    """Run the locations extractor job in-process."""
    run_job_in_process("locations")

from itertools import tee

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.transform import transform_ldap_persons_with_query_to_mex_persons
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
)
from mex.common.types import MergedOrganizationalUnitIdentifier, MergedPersonIdentifier
from mex.extractors.confluence_vvt.extract import (
    extract_confluence_vvt_authors,
    fetch_all_data_page_ids,
    fetch_all_pages_data,
)
from mex.extractors.confluence_vvt.models import ConfluenceVvtSource
from mex.extractors.confluence_vvt.transform import (
    transform_confluence_vvt_sources_to_extracted_activities,
)
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="confluence_vvt")
def confluence_vvt_sources(
    extracted_primary_source_confluence_vvt: ExtractedPrimarySource,
) -> list[ConfluenceVvtSource]:
    """Extract Confluence VVT sources."""
    page_ids = fetch_all_data_page_ids()
    unfiltered_confluence_vvt_sources = fetch_all_pages_data(page_ids)
    return list(
        filter_by_global_rules(
            extracted_primary_source_confluence_vvt.identifier,
            unfiltered_confluence_vvt_sources,
        )
    )


@asset(group_name="confluence_vvt", deps=["extracted_primary_source_mex"])
def extracted_primary_source_confluence_vvt(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return Confluence VVT primary source."""
    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "confluence-vvt"
    )
    load([extracted_primary_source])
    return extracted_primary_source


@asset(group_name="confluence_vvt")
def extracted_confluence_vvt_person_ids_by_query_string(
    confluence_vvt_sources: list[ConfluenceVvtSource],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Return mapping from query string to person IDs.

    Transforms and loads Confluence VVT persons along the way.
    """
    ldap_authors = extract_confluence_vvt_authors(confluence_vvt_sources)
    ldap_author_gens = tee(ldap_authors, 2)
    mex_authors = transform_ldap_persons_with_query_to_mex_persons(
        ldap_author_gens[0],
        extracted_primary_source_ldap,
        extracted_organizational_units,
    )
    load(mex_authors)

    return get_merged_ids_by_query_string(
        ldap_author_gens[1], extracted_primary_source_ldap
    )


@asset(group_name="confluence_vvt")
def extracted_confluence_vvt_activities(
    confluence_vvt_sources: list[ConfluenceVvtSource],
    extracted_confluence_vvt_person_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
    extracted_primary_source_confluence_vvt: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedActivity]:
    """Transform and load Confluence VVT activities."""
    mex_activities = list(
        transform_confluence_vvt_sources_to_extracted_activities(
            confluence_vvt_sources,
            extracted_primary_source_confluence_vvt,
            extracted_confluence_vvt_person_ids_by_query_string,
            unit_stable_target_ids_by_synonym,
        )
    )
    load(mex_activities)
    return mex_activities


@entrypoint(Settings)
def run() -> None:
    """Run the confluence-vvt extractor job in-process."""
    run_job_in_process("confluence_vvt")

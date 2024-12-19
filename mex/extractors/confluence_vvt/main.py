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
    fetch_all_vvt_pages_ids,
    get_all_persons_from_all_pages,
    get_all_units_from_all_pages,
    get_page_data_by_id,
)
from mex.extractors.confluence_vvt.models import ConfluenceVvtPage
from mex.extractors.confluence_vvt.transform import (
    transform_confluence_vvt_activities_to_extracted_activities,
)
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.mapping.transform import transform_mapping_data_to_model
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="confluence_vvt")
def confluence_vvt_pages(
    extracted_primary_source_confluence_vvt: ExtractedPrimarySource,
) -> list[ConfluenceVvtPage]:
    """Extract Confluence VVT sources."""
    page_ids = fetch_all_vvt_pages_ids()
    unfiltered_activities = get_page_data_by_id(page_ids)
    return list(
        filter_by_global_rules(
            extracted_primary_source_confluence_vvt.identifier,
            unfiltered_activities,
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
def activity_mapping() -> AnyMappingModel:
    """Return activity mapping."""
    settings = Settings.get()
    return transform_mapping_data_to_model(
        extract_mapping_data(
            settings.confluence_vvt.template_v1_mapping_path / "activity.yaml"
        ),
        ExtractedActivity,
    )


@asset(group_name="confluence_vvt")
def extracted_confluence_vvt_person_ids_by_query_string(
    confluence_vvt_pages: list[ConfluenceVvtPage],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    activity_mapping: AnyMappingModel,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Return mapping from query string to person IDs.

    Transforms and loads Confluence VVT persons along the way.
    """
    contacts = get_all_persons_from_all_pages(confluence_vvt_pages, activity_mapping)
    involved_persons = get_all_units_from_all_pages(
        confluence_vvt_pages, activity_mapping
    )

    ldap_authors = extract_confluence_vvt_authors(contacts + involved_persons)
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
    confluence_vvt_pages: list[ConfluenceVvtPage],
    extracted_confluence_vvt_person_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
    extracted_primary_source_confluence_vvt: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    activity_mapping: AnyMappingModel,
) -> list[ExtractedActivity]:
    """Transform and load Confluence VVT activities."""
    mex_activities = list(
        transform_confluence_vvt_activities_to_extracted_activities(
            confluence_vvt_pages,
            extracted_primary_source_confluence_vvt,
            activity_mapping,
            extracted_confluence_vvt_person_ids_by_query_string,
            unit_stable_target_ids_by_synonym,
        )
    )
    load(mex_activities)
    return mex_activities


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the confluence-vvt extractor job in-process."""
    run_job_in_process("confluence_vvt")

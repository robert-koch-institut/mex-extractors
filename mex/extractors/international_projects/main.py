from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.transform import (
    transform_ldap_persons_with_query_to_extracted_persons,
)
from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.international_projects.extract import (
    extract_international_projects_funding_sources,
    extract_international_projects_partner_organizations,
    extract_international_projects_project_leaders,
    extract_international_projects_sources,
)
from mex.extractors.international_projects.models.source import (
    InternationalProjectsSource,
)
from mex.extractors.international_projects.transform import (
    transform_international_projects_sources_to_extracted_activities,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="international_projects")
def international_projects_sources() -> list[InternationalProjectsSource]:
    """Extract from international project sources."""
    return filter_by_global_rules(
        get_extracted_primary_source_id_by_name("international-projects"),
        extract_international_projects_sources(),
    )


@asset(group_name="international_projects")
def international_projects_person_ids_by_query_str(
    international_projects_sources: list[InternationalProjectsSource],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Transform LDAP persons to extracted persons and group their IDs by query."""
    ldap_project_leaders = list(
        extract_international_projects_project_leaders(international_projects_sources)
    )
    mex_authors = transform_ldap_persons_with_query_to_extracted_persons(
        ldap_project_leaders,
        get_extracted_primary_source_id_by_name("ldap"),
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_authors)
    return get_merged_ids_by_query_string(
        ldap_project_leaders, get_extracted_primary_source_id_by_name("ldap")
    )


@asset(group_name="international_projects")
def international_projects_funding_sources_ids_by_query_string(
    international_projects_sources: list[InternationalProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract funding sources and return their stable target IDs by query string."""
    return extract_international_projects_funding_sources(
        international_projects_sources
    )


@asset(group_name="international_projects")
def international_projects_partner_organization_ids_by_query_string(
    international_projects_sources: list[InternationalProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract partner organizations and return their IDs grouped by query string."""
    return extract_international_projects_partner_organizations(
        international_projects_sources
    )


@asset(group_name="international_projects")
def international_projects_extracted_activities(
    international_projects_sources: list[InternationalProjectsSource],
    international_projects_person_ids_by_query_str: dict[
        str, list[MergedPersonIdentifier]
    ],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    international_projects_funding_sources_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    international_projects_partner_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
) -> list[ExtractedActivity]:
    """Transform projects to extracted activities, load and return them."""
    settings = Settings.get()
    activity = ActivityMapping.model_validate(
        load_yaml(settings.international_projects.mapping_path / "activity.yaml")
    )
    mex_sources = list(
        transform_international_projects_sources_to_extracted_activities(
            international_projects_sources,
            activity,
            international_projects_person_ids_by_query_str,
            unit_stable_target_ids_by_synonym,
            international_projects_funding_sources_ids_by_query_string,
            international_projects_partner_organization_ids_by_query_string,
        )
    )
    load(mex_sources)
    return mex_sources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the international-projects extractor job in-process."""
    run_job_in_process("international_projects")

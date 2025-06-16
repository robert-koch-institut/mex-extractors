from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.transform import transform_ldap_persons_with_query_to_mex_persons
from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.ff_projects.extract import (
    extract_ff_project_authors,
    extract_ff_projects_organizations,
    extract_ff_projects_sources,
)
from mex.extractors.ff_projects.filter import (
    filter_and_log_ff_projects_sources,
    filter_out_duplicate_source_ids,
)
from mex.extractors.ff_projects.models.source import FFProjectsSource
from mex.extractors.ff_projects.transform import (
    transform_ff_projects_source_to_extracted_activity,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="ff_projects", deps=["extracted_primary_source_mex"])
def extracted_primary_source_ff_projects(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return FF Projects extracted primary source."""
    (extracted_primary_source_ff_projects,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "ff-projects",
    )
    load([extracted_primary_source_ff_projects])
    return extracted_primary_source_ff_projects


@asset(group_name="ff_projects")
def ff_projects_sources(
    extracted_primary_source_ff_projects: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[FFProjectsSource]:
    """Extract FF Projects sources and filter out invalid items."""
    ff_projects_sources = extract_ff_projects_sources()
    filtered_sources = filter_out_duplicate_source_ids(ff_projects_sources)
    return filter_and_log_ff_projects_sources(
        filtered_sources,
        extracted_primary_source_ff_projects.stableTargetId,
        unit_stable_target_ids_by_synonym,
    )


@asset(group_name="ff_projects")
def ff_projects_person_ids_by_query_string(
    ff_projects_sources: list[FFProjectsSource],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, list[MergedPersonIdentifier]]:
    """Extract authors for FF Projects from LDAP and group them by query."""
    ff_projects_authors = list(extract_ff_project_authors(ff_projects_sources))
    extracted_persons = transform_ldap_persons_with_query_to_mex_persons(
        ff_projects_authors,
        extracted_primary_source_ldap,
        extracted_organizational_units,
    )
    load(extracted_persons)
    return {
        str(query_string): [MergedPersonIdentifier(id_) for id_ in merged_ids]
        for query_string, merged_ids in get_merged_ids_by_query_string(
            ff_projects_authors, extracted_primary_source_ldap
        ).items()
    }


@asset(group_name="ff_projects")
def ff_projects_organization_ids_by_query_string(
    ff_projects_sources: list[FFProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for FF Projects from wikidata and group them by query."""
    return extract_ff_projects_organizations(ff_projects_sources)


@asset(group_name="ff_projects")
def extract_ff_projects(
    ff_projects_sources: list[FFProjectsSource],
    extracted_primary_source_ff_projects: ExtractedPrimarySource,
    ff_projects_person_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    ff_projects_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
) -> list[ExtractedActivity]:
    """Transform FF Projects to extracted activities and load them to the sinks."""
    settings = Settings.get()
    ff_projects_activity = ActivityMapping.model_validate(
        load_yaml(settings.ff_projects.mapping_path / "activity.yaml"),
    )
    extracted_activities = [
        transform_ff_projects_source_to_extracted_activity(
            ff_projects_source,
            extracted_primary_source_ff_projects,
            ff_projects_person_ids_by_query_string,
            unit_stable_target_ids_by_synonym,
            ff_projects_organization_ids_by_query_string,
            ff_projects_activity,
        )
        for ff_projects_source in ff_projects_sources
    ]
    load(extracted_activities)
    return extracted_activities


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the ff-projects extractor job in-process."""
    run_job_in_process("ff_projects")

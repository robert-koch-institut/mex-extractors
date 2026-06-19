from dagster import AssetExecutionContext, asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
)
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.ff_projects.extract import (
    extract_ff_project_author_merged_ids,
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


@asset(group_name="ff_projects")
def ff_projects_sources() -> list[FFProjectsSource]:
    """Extract FF Projects sources and filter out invalid items."""
    ff_projects_sources = extract_ff_projects_sources()
    filtered_sources = filter_out_duplicate_source_ids(ff_projects_sources)
    return filter_and_log_ff_projects_sources(
        filtered_sources,
    )


@asset(group_name="ff_projects")
def ff_projects_person_ids_by_query_str(
    ff_projects_sources: list[FFProjectsSource],
) -> dict[str, MergedPersonIdentifier]:
    """Extract authors for FF Projects from LDAP and group them by query."""
    return extract_ff_project_author_merged_ids(ff_projects_sources)


@asset(group_name="ff_projects")
def ff_projects_organization_ids_by_query_str(
    ff_projects_sources: list[FFProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for FF Projects from wikidata and group them by query."""
    return extract_ff_projects_organizations(ff_projects_sources)


@asset(group_name="ff_projects", metadata={"entity_type": "activity"})
def ff_projects_activities(
    context: AssetExecutionContext,
    ff_projects_sources: list[FFProjectsSource],
    ff_projects_person_ids_by_query_str: dict[str, MergedPersonIdentifier],
    ff_projects_organization_ids_by_query_str: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedActivity]:
    """Transform FF Projects to extracted activities and load them to the sinks."""
    settings = Settings.get()
    ff_projects_activity = ActivityMapping.model_validate(
        load_yaml(settings.ff_projects.mapping_path / "activity.yaml"),
    )
    extracted_activities = [
        transform_ff_projects_source_to_extracted_activity(
            ff_projects_source,
            ff_projects_person_ids_by_query_str,
            ff_projects_organization_ids_by_query_str,
            ff_projects_activity,
        )
        for ff_projects_source in ff_projects_sources
    ]
    load(extracted_activities)
    context.add_output_metadata({"num_items": len(extracted_activities)})
    return extracted_activities


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the ff-projects extractor job in-process."""
    run_job_in_process("ff_projects")

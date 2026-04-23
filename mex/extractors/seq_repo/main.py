from dagster import AssetExecutionContext, asset

from mex.common.cli import entrypoint
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import MergedPersonIdentifier
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.seq_repo.extract import (
    extract_source_project_coordinator_by_name,
    extract_sources,
)
from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
    transform_seq_repo_resource_to_extracted_resource,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="seq_repo")
def seq_repo_source() -> list[SeqRepoSource]:
    """Extract sources from seq-repo."""
    return extract_sources()


@asset(group_name="seq_repo")
def seq_repo_latest_source(
    seq_repo_source: list[SeqRepoSource],
) -> dict[str, SeqRepoSource]:
    """Filter latest sources from seq-repo source."""
    return filter_sources_on_latest_sequencing_date(
        seq_repo_source,
    )


@asset(group_name="seq_repo")
def seq_repo_merged_person_ids_by_name(
    seq_repo_latest_source: dict[str, SeqRepoSource],
) -> dict[str, MergedPersonIdentifier]:
    """Extract source project coordinators."""
    return extract_source_project_coordinator_by_name(seq_repo_latest_source)


@asset(group_name="seq_repo", metadata={"entity_type": "organization"})
def seq_repo_extracted_activities_by_id_str(
    context: AssetExecutionContext,
    seq_repo_latest_source: dict[str, SeqRepoSource],
    seq_repo_merged_person_ids_by_query_string: dict[str, MergedPersonIdentifier],
) -> dict[str, ExtractedActivity]:
    """Extract activities from seq-repo."""
    settings = Settings.get()
    activity = ActivityMapping.model_validate(
        load_yaml(settings.seq_repo.mapping_path / "activity.yaml")
    )

    mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_latest_source,
        activity,
        seq_repo_merged_person_ids_by_query_string,
    )
    load(mex_activities)
    activities_by_id_str = {
        activity.identifierInPrimarySource: activity for activity in mex_activities
    }
    context.add_output_metadata({"num_items": len(mex_activities)})
    return activities_by_id_str


@asset(group_name="seq_repo")
def seq_repo_extracted_access_platform() -> ExtractedAccessPlatform:
    """Extract access platform from seq-repo."""
    settings = Settings.get()
    access_platform = AccessPlatformMapping.model_validate(
        load_yaml(settings.seq_repo.mapping_path / "access-platform.yaml")
    )
    mex_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            access_platform,
        )
    )
    load([mex_access_platform])
    return mex_access_platform


@asset(group_name="seq_repo", metadata={"entity_type": "resource"})
def seq_repo_resources(  # noqa: PLR0913
    context: AssetExecutionContext,
    seq_repo_latest_source: dict[str, SeqRepoSource],
    seq_repo_extracted_activities_by_id_str: dict[str, ExtractedActivity],
    seq_repo_extracted_access_platform: ExtractedAccessPlatform,
    seq_repo_merged_person_ids_by_query_string: dict[str, MergedPersonIdentifier],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Extract resources from seq-repo."""
    settings = Settings.get()
    resource = ResourceMapping.model_validate(
        load_yaml(settings.seq_repo.mapping_path / "resource.yaml")
    )

    resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_latest_source,
        seq_repo_extracted_activities_by_id_str,
        seq_repo_extracted_access_platform,
        resource,
        seq_repo_merged_person_ids_by_query_string,
        extracted_organization_rki,
    )
    load(resources)
    context.add_output_metadata({"num_items": len(resources)})
    return resources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the seq-repo extractor job in-process."""
    run_job_in_process("seq_repo")

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import MergedActivity, MergedOrganizationalUnit
from mex.extractors.datenkompass.extract import get_merged_items
from mex.extractors.datenkompass.filter import filter_for_bmg
from mex.extractors.datenkompass.load import write_activity_to_json
from mex.extractors.datenkompass.source import DatenkompassActivity
from mex.extractors.datenkompass.transform import transform_to_target_fields
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="datenkompass")
def extracted_and_filtered_merged_activities() -> list[MergedActivity]:
    """Get merged items from mex-backend and filter them by allow-list."""
    entity_type = ["MergedActivity"]
    had_primary_source = ["fR1KAfaMWLMUdvcR1KYMLA"]  # Synopse
    merged_activities = list(get_merged_items(entity_type, had_primary_source))

    return filter_for_bmg(merged_activities)


@asset(group_name="datenkompass")
def transform_activities_to_target_fields(
    extracted_and_filtered_merged_activities: list[MergedActivity],
) -> list[DatenkompassActivity]:
    """Get merged items from mex-backend and filter them by allow-list."""
    merged_units = list(get_merged_items(["MergedOrganizationalUnit"], None))

    return transform_to_target_fields(
        extracted_and_filtered_merged_activities,
        [MergedOrganizationalUnit.model_validate(unit) for unit in merged_units],
    )


@asset(group_name="datenkompass")
def publish_activities(
    transform_activities_to_target_fields: list[DatenkompassActivity],
) -> None:
    """Write received merged items to configured sink."""
    write_activity_to_json(transform_activities_to_target_fields)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

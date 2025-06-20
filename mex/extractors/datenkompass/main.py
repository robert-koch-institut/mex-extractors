from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import MergedActivity, MergedOrganizationalUnit
from mex.extractors.datenkompass.extract import (
    get_merged_items,
    get_relevant_primary_source_ids,
)
from mex.extractors.datenkompass.filter import filter_for_bmg
from mex.extractors.datenkompass.item import AnyDatenkompassModel, DatenkompassActivity
from mex.extractors.datenkompass.load import write_activity_to_json
from mex.extractors.datenkompass.transform import transform_to_target_fields
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="datenkompass")
def extracted_and_filtered_merged_activities() -> list[MergedActivity]:
    """Get merged items and filter them."""
    relevant_primary_sources = [
        "blueant",
        "confluence-vvt",
        "datscha-web",
        "ff-projects",
        "international-projects",
        "report-server",  # synopse
    ]
    entity_type = ["MergedActivity"]
    had_primary_source = get_relevant_primary_source_ids(relevant_primary_sources)
    merged_activities = list(get_merged_items(None, entity_type, had_primary_source))

    return filter_for_bmg(merged_activities)


@asset(group_name="datenkompass")
def transform_activities_to_target_fields(
    extracted_and_filtered_merged_activities: list[MergedActivity],
) -> list[DatenkompassActivity]:
    """Transform items to datenkompass items."""
    merged_units = list(get_merged_items(None, ["MergedOrganizationalUnit"], None))

    return transform_to_target_fields(
        extracted_and_filtered_merged_activities,
        [MergedOrganizationalUnit.model_validate(unit) for unit in merged_units],
    )


@asset(group_name="datenkompass")
def publish_activities(
    transform_activities_to_target_fields: list[AnyDatenkompassModel],
) -> None:
    """Write items to S3."""
    write_activity_to_json(transform_activities_to_target_fields)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

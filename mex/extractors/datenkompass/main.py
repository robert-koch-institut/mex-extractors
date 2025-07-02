from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganizationalUnit,
)
from mex.common.types import (
    MergedOrganizationIdentifier,
)
from mex.extractors.datenkompass.extract import (
    get_merged_items,
    get_relevant_primary_source_ids,
)
from mex.extractors.datenkompass.filter import filter_for_bmg
from mex.extractors.datenkompass.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
)
from mex.extractors.datenkompass.load import start_s3_client, write_item_to_json
from mex.extractors.datenkompass.transform import (
    transform_activities,
    transform_biblio_resources,
)
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
def extracted_and_filtered_merged_biblio_resource() -> list[
    MergedBibliographicResource
]:
    """Get merged items and filter them."""
    relevant_primary_sources = ["endnote"]
    entity_type = ["MergedBibliographicResource"]
    had_primary_source = get_relevant_primary_source_ids(relevant_primary_sources)
    merged_biblio_resource = list(
        get_merged_items(None, entity_type, had_primary_source)
    )

    return [
        MergedBibliographicResource.model_validate(item)
        for item in merged_biblio_resource
    ]


@asset(group_name="datenkompass")
def extracted_merged_organizational_units() -> list[MergedOrganizationalUnit]:
    """Get all organizational units."""
    return [
        MergedOrganizationalUnit.model_validate(unit)
        for unit in get_merged_items(None, ["MergedOrganizationalUnit"], None)
    ]


@asset(group_name="datenkompass")
def extracted_merged_bmg_ids() -> list[MergedOrganizationIdentifier]:
    """Get all organizational units."""
    return list(
        {
            MergedOrganizationIdentifier(bmg.identifier)
            for bmg in get_merged_items("BMG", ["MergedOrganization"], None)
        }
    )


@asset(group_name="datenkompass")
def transform_activities_to_target_fields(
    extracted_and_filtered_merged_activities: list[MergedActivity],
    extracted_merged_organizational_units: list[MergedOrganizationalUnit],
    extracted_merged_bmg_ids: list[MergedOrganizationIdentifier],
) -> list[DatenkompassActivity]:
    """Transform items to datenkompass items."""
    return transform_activities(
        extracted_and_filtered_merged_activities,
        extracted_merged_organizational_units,
        extracted_merged_bmg_ids,
    )


@asset(group_name="datenkompass")
def transform_biblio_resources_to_target_fields(
    extracted_and_filtered_merged_biblio_resource: list[MergedBibliographicResource],
    extracted_merged_organizational_units: list[MergedOrganizationalUnit],
) -> list[DatenkompassBibliographicResource]:
    """Transform items to datenkompass items."""
    return transform_biblio_resources(
        extracted_and_filtered_merged_biblio_resource,
        extracted_merged_organizational_units,
    )


@asset(group_name="datenkompass")
def publish_activities(
    transform_activities_to_target_fields: list[DatenkompassActivity],
    transform_biblio_resources_to_target_fields: list[
        DatenkompassBibliographicResource
    ],
) -> None:
    """Write items to S3."""
    s3_client = start_s3_client()
    write_item_to_json(transform_activities_to_target_fields, s3_client)
    write_item_to_json(transform_biblio_resources_to_target_fields, s3_client)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

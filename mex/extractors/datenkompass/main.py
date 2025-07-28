from typing import cast

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.datenkompass.extract import (
    get_merged_items,
    get_relevant_primary_source_ids,
)
from mex.extractors.datenkompass.filter import filter_for_bmg
from mex.extractors.datenkompass.load import start_s3_client, write_item_to_json
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
)
from mex.extractors.datenkompass.transform import (
    transform_activities,
    transform_bibliographic_resources,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="datenkompass")
def extracted_merged_organizational_units_by_id() -> dict[
    MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
]:
    """Get all organizational units as dictionary by id."""
    return {
        organization.identifier: organization
        for organization in cast(
            "list[MergedOrganizationalUnit]",
            get_merged_items(None, ["MergedOrganizationalUnit"], None),
        )
    }


@asset(group_name="datenkompass")
def extracted_merged_bmg_ids() -> set[MergedOrganizationIdentifier]:
    """Get BMG identifiers as set."""
    return {
        bmg.identifier
        for bmg in cast(
            "list[MergedOrganization]",
            get_merged_items("BMG", ["MergedOrganization"], None),
        )
    }


@asset(group_name="datenkompass")
def person_name_by_id() -> dict[MergedPersonIdentifier, list[str]]:
    """Get all person names as dictionary by id."""
    return {
        person.identifier: person.fullName
        for person in cast(
            "list[MergedPerson]", get_merged_items(None, ["MergedPerson"], None)
        )
    }


@asset(group_name="datenkompass")
def extracted_merged_activities() -> list[MergedActivity]:
    """Get merged activities."""
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
    return cast(
        "list[MergedActivity]", get_merged_items(None, entity_type, had_primary_source)
    )


@asset(group_name="datenkompass")
def extracted_and_filtered_merged_activities(
    extracted_merged_activities: list[MergedActivity],
    extracted_merged_bmg_ids: set[MergedOrganizationIdentifier],
) -> list[MergedActivity]:
    """Filter merged activities."""
    return filter_for_bmg(extracted_merged_activities, extracted_merged_bmg_ids)


@asset(group_name="datenkompass")
def extracted_merged_bibliographic_resources() -> list[MergedBibliographicResource]:
    """Get merged bibliographic resources."""
    relevant_primary_sources = ["endnote"]
    entity_type = ["MergedBibliographicResource"]
    had_primary_source = get_relevant_primary_source_ids(relevant_primary_sources)
    return cast(
        "list[MergedBibliographicResource]",
        get_merged_items(None, entity_type, had_primary_source),
    )


@asset(group_name="datenkompass")
def transform_activities_to_datenkompass_activities(
    extracted_and_filtered_merged_activities: list[MergedActivity],
    extracted_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[DatenkompassActivity]:
    """Transform activities to datenkompass items."""
    return transform_activities(
        extracted_and_filtered_merged_activities,
        extracted_merged_organizational_units_by_id,
    )


@asset(group_name="datenkompass")
def transform_bibliographic_resources_to_datenkompass_bibliographic_resources(
    extracted_merged_bibliographic_resources: list[MergedBibliographicResource],
    extracted_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    person_name_by_id: dict[MergedPersonIdentifier, list[str]],
) -> list[DatenkompassBibliographicResource]:
    """Transform bibliographic resources to datenkompass items."""
    return transform_bibliographic_resources(
        extracted_merged_bibliographic_resources,
        extracted_merged_organizational_units_by_id,
        person_name_by_id,
    )


@asset(group_name="datenkompass")
def load_activities(
    transform_activities_to_datenkompass_activities: list[DatenkompassActivity],
    transform_bibliographic_resources_to_datenkompass_bibliographic_resources: list[
        DatenkompassBibliographicResource
    ],
) -> None:
    """Write items to S3."""
    s3_client = start_s3_client()
    write_item_to_json(transform_activities_to_datenkompass_activities, s3_client)
    write_item_to_json(
        transform_bibliographic_resources_to_datenkompass_bibliographic_resources,
        s3_client,
    )


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

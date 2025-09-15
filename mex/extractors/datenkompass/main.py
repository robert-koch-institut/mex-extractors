from collections import deque
from typing import cast

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedContactPoint,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedResource,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.datenkompass.extract import (
    get_merged_items,
    get_relevant_primary_source_ids,
)
from mex.extractors.datenkompass.filter import (
    filter_for_organization,
    find_descendant_units,
)
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
    DatenkompassResource,
)
from mex.extractors.datenkompass.transform import (
    transform_activities,
    transform_bibliographic_resources,
    transform_resources,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3XlsxSink


@asset(group_name="datenkompass")
def fetched_merged_organizational_units_by_id() -> dict[
    MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
]:
    """Get all organizational units as dictionary by id."""
    return {
        organization.identifier: organization
        for organization in cast(
            "list[MergedOrganizationalUnit]",
            get_merged_items(entity_type=["MergedOrganizationalUnit"]),
        )
    }


@asset(group_name="datenkompass")
def filtered_merged_organizational_unit_ids(
    fetched_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Get child unit ids by filter setting for extraction filter."""
    return find_descendant_units(fetched_merged_organizational_units_by_id)


@asset(group_name="datenkompass")
def fetched_merged_contact_points_by_id() -> dict[
    MergedContactPointIdentifier, MergedContactPoint
]:
    """Get all contact points as dict by id."""
    return {
        cp.identifier: cp
        for cp in cast(
            "list[MergedContactPoint]",
            get_merged_items(entity_type=["MergedContactPoint"]),
        )
    }


@asset(group_name="datenkompass")
def filtered_merged_organization_ids() -> set[MergedOrganizationIdentifier]:
    """Get relevant organization identifiers as set."""
    settings = Settings()
    return {
        organization.identifier
        for organization in cast(
            "list[MergedOrganization]",
            get_merged_items(
                query_string=settings.datenkompass.organization_filter,
                entity_type=["MergedOrganization"],
            ),
        )
    }


@asset(group_name="datenkompass")
def person_name_by_id() -> dict[MergedPersonIdentifier, str]:
    """Get person name as dictionary by id."""
    return {
        person.identifier: (
            person.fullName[0]
            if person.fullName
            else (
                person.familyName[0]
                + (f", {person.givenName[0]}" if person.givenName else "")
            )
        )
        for person in cast(
            "list[MergedPerson]", get_merged_items(entity_type=["MergedPerson"])
        )
        if person.fullName or person.familyName
    }


@asset(group_name="datenkompass")
def fetched_merged_activities() -> list[MergedActivity]:
    """Get merged activities."""
    relevant_primary_sources = [
        "blueant",
        # "confluence-vvt",
        # "datscha-web",
        # "ff-projects",
        # "international-projects",
        # "report-server",  # synopse
    ]
    entity_type = ["MergedActivity"]
    primary_source_ids = get_relevant_primary_source_ids(relevant_primary_sources)
    return cast(
        "list[MergedActivity]",
        get_merged_items(
            entity_type=entity_type,
            referenced_identifier=primary_source_ids,
            reference_field="hadPrimarySource",
        ),
    )


@asset(group_name="datenkompass")
def extracted_and_filtered_merged_activities(
    fetched_merged_activities: list[MergedActivity],
    filtered_merged_organization_ids: set[MergedOrganizationIdentifier],
) -> list[MergedActivity]:
    """Filter merged activities."""
    return filter_for_organization(
        fetched_merged_activities, filtered_merged_organization_ids
    )


@asset(group_name="datenkompass")
def fetched_merged_bibliographic_resources() -> list[MergedBibliographicResource]:
    """Get merged bibliographic resources."""
    relevant_primary_sources = ["endnote"]
    entity_type = ["MergedBibliographicResource"]
    primary_source_ids = get_relevant_primary_source_ids(relevant_primary_sources)
    return cast(
        "list[MergedBibliographicResource]",
        get_merged_items(
            entity_type=entity_type,
            referenced_identifier=primary_source_ids,
            reference_field="hadPrimarySource",
        ),
    )


@asset(group_name="datenkompass")
def fetched_merged_resources_by_primary_source(
    filtered_merged_organizational_unit_ids: list[str],
) -> dict[str, list[MergedResource]]:
    """Get merged resources as dictionary."""
    relevant_primary_sources = ["open-data"]  # , "report-server"]
    entity_type = ["MergedResource"]
    merged_resources_by_primary_source: dict[str, list[MergedResource]] = {}
    for rps in relevant_primary_sources:
        primary_source_ids = get_relevant_primary_source_ids([rps])

        merged_resources_by_primary_source[rps] = cast(
            "list[MergedResource]",
            get_merged_items(
                entity_type=entity_type,
                referenced_identifier=primary_source_ids,
                reference_field="hadPrimarySource",
            ),
        )

    first_fetched_merged_resources_ids = {
        mr.identifier
        for merged_resources in merged_resources_by_primary_source.values()
        for mr in merged_resources
    }

    merged_resources_of_units = cast(
        "list[MergedResource]",
        get_merged_items(
            entity_type=entity_type,
            referenced_identifier=filtered_merged_organizational_unit_ids,
            reference_field="unitInCharge",
        ),
    )

    merged_resources_by_primary_source["unit filter"] = [
        mr
        for mr in merged_resources_of_units
        if mr.identifier not in first_fetched_merged_resources_ids
    ]

    return merged_resources_by_primary_source


@asset(group_name="datenkompass")
def transform_activities_to_datenkompass_activities(
    extracted_and_filtered_merged_activities: list[MergedActivity],
    fetched_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[DatenkompassActivity]:
    """Transform activities to datenkompass items."""
    return transform_activities(
        extracted_and_filtered_merged_activities,
        fetched_merged_organizational_units_by_id,
    )


@asset(group_name="datenkompass")
def transform_bibliographic_resources_to_datenkompass_bibliographic_resources(
    fetched_merged_bibliographic_resources: list[MergedBibliographicResource],
    fetched_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    person_name_by_id: dict[MergedPersonIdentifier, str],
) -> list[DatenkompassBibliographicResource]:
    """Transform bibliographic resources to datenkompass items."""
    return transform_bibliographic_resources(
        fetched_merged_bibliographic_resources,
        fetched_merged_organizational_units_by_id,
        person_name_by_id,
    )


@asset(group_name="datenkompass")
def transform_resources_to_datenkompass_resources(
    fetched_merged_resources_by_primary_source: dict[str, list[MergedResource]],
    fetched_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    fetched_merged_contact_points_by_id: dict[
        MergedContactPointIdentifier, MergedContactPoint
    ],
) -> list[DatenkompassResource]:
    """Transform resources to datenkompass items."""
    return transform_resources(
        fetched_merged_resources_by_primary_source,
        fetched_merged_organizational_units_by_id,
        fetched_merged_contact_points_by_id,
    )


@asset(group_name="datenkompass")
def publish_to_s3_xlsx(
    transform_activities_to_datenkompass_activities: list[DatenkompassActivity],
    transform_bibliographic_resources_to_datenkompass_bibliographic_resources: list[
        DatenkompassBibliographicResource
    ],
    transform_resources_to_datenkompass_resources: list[DatenkompassResource],
) -> None:
    """Write items to S3 xlsx."""
    settings = Settings.get()
    sep = settings.datenkompass.list_delimiter
    s3xlsx = S3XlsxSink(separator=sep, sort_columns_alphabetically=False)
    deque(s3xlsx.load(transform_activities_to_datenkompass_activities), maxlen=0)
    deque(
        s3xlsx.load(
            transform_bibliographic_resources_to_datenkompass_bibliographic_resources,
        ),
        maxlen=0,
    )
    deque(s3xlsx.load(transform_resources_to_datenkompass_resources), maxlen=0)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

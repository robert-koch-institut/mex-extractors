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
from mex.extractors.datenkompass.load import (
    start_s3_client,
    write_items_to_xlsx,
)
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
    DatenkompassResource,
)
from mex.extractors.datenkompass.models.mapping import DatenkompassMapping
from mex.extractors.datenkompass.transform import (
    transform_activities,
    transform_bibliographic_resources,
    transform_resources,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


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
    settings = Settings.get()
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
def activity_mapping() -> DatenkompassMapping:
    """Load the Datenkompass activity mapping."""
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "activity.yaml")
    )


@asset(group_name="datenkompass")
def bibliographic_resource_mapping() -> DatenkompassMapping:
    """Load the Datenkompass activity mapping."""
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "bibliographic-resource.yaml")
    )


@asset(group_name="datenkompass")
def resource_mapping() -> DatenkompassMapping:
    """Load the Datenkompass activity mapping."""
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "resource.yaml")
    )


@asset(group_name="datenkompass")
def fetched_merged_activities(
    activity_mapping: DatenkompassMapping,
) -> list[MergedActivity]:
    """Get merged activities."""
    relevant_primary_sources = activity_mapping.fields[0].mappingRules[0].forValues
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
def fetched_merged_bibliographic_resources(
    bibliographic_resource_mapping: DatenkompassMapping,
) -> list[MergedBibliographicResource]:
    """Get merged bibliographic resources."""
    relevant_primary_sources = (
        bibliographic_resource_mapping.fields[0].mappingRules[0].forValues
    )
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
    resource_mapping: DatenkompassMapping,
) -> dict[str, list[MergedResource]]:
    """Get merged resources as dictionary."""
    relevant_primary_sources = resource_mapping.fields[0].mappingRules[0].forValues
    entity_type = ["MergedResource"]
    merged_resources_by_primary_source: dict[str, list[MergedResource]] = {}
    if relevant_primary_sources:
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
    activity_mapping: DatenkompassMapping,
) -> list[DatenkompassActivity]:
    """Transform activities to datenkompass items."""
    return transform_activities(
        extracted_and_filtered_merged_activities,
        fetched_merged_organizational_units_by_id,
        activity_mapping,
    )


@asset(group_name="datenkompass")
def transform_bibliographic_resources_to_datenkompass_bibliographic_resources(
    fetched_merged_bibliographic_resources: list[MergedBibliographicResource],
    fetched_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    person_name_by_id: dict[MergedPersonIdentifier, str],
    bibliographic_resource_mapping: DatenkompassMapping,
) -> list[DatenkompassBibliographicResource]:
    """Transform bibliographic resources to datenkompass items."""
    return transform_bibliographic_resources(
        fetched_merged_bibliographic_resources,
        fetched_merged_organizational_units_by_id,
        person_name_by_id,
        bibliographic_resource_mapping,
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
    resource_mapping: DatenkompassMapping,
) -> list[DatenkompassResource]:
    """Transform resources to datenkompass items."""
    return transform_resources(
        fetched_merged_resources_by_primary_source,
        fetched_merged_organizational_units_by_id,
        fetched_merged_contact_points_by_id,
        resource_mapping,
    )


@asset(group_name="datenkompass")
def load_activities(
    transform_activities_to_datenkompass_activities: list[DatenkompassActivity],
    transform_bibliographic_resources_to_datenkompass_bibliographic_resources: list[
        DatenkompassBibliographicResource
    ],
    transform_resources_to_datenkompass_resources: list[DatenkompassResource],
) -> None:
    """Write items to S3."""
    s3_client = start_s3_client()
    write_items_to_xlsx(transform_activities_to_datenkompass_activities, s3_client)
    write_items_to_xlsx(
        transform_bibliographic_resources_to_datenkompass_bibliographic_resources,
        s3_client,
    )
    write_items_to_xlsx(transform_resources_to_datenkompass_resources, s3_client)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

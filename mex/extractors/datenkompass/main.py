from collections import deque
from typing import cast

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    MergedActivity,
    MergedContactPoint,
    MergedOrganizationalUnit,
    MergedResource,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
)
from mex.extractors.datenkompass.extract import (
    get_filtered_primary_source_ids,
    get_merged_items,
)
from mex.extractors.datenkompass.filter import (
    filter_activities_by_organization,
    filter_merged_items_for_primary_source,
    filter_merged_resources_by_unit,
)
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassResource,
)
from mex.extractors.datenkompass.models.mapping import (
    DatenkompassFilterMapping,
)
from mex.extractors.datenkompass.transform import (
    transform_activities,
    transform_resources,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3XlsxSink
from mex.extractors.utils import load_yaml


@asset(group_name="datenkompass")
def datenkompass_merged_organizational_units_by_id() -> dict[
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
def datenkompass_merged_contact_points_by_id() -> dict[
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


'''@asset(group_name="datenkompass")
def datenkompass_person_name_str_by_id() -> dict[MergedPersonIdentifier, str]:
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
'''


@asset(group_name="datenkompass")
def datenkompass_activity_filter_mapping() -> DatenkompassFilterMapping:
    """Get filter for datenkompass activities."""
    settings = Settings.get()
    return DatenkompassFilterMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "activity_filter.yaml")
    )


'''@asset(group_name="datenkompass")
def datenkompass_bibliographic_resource_mapping() -> DatenkompassMapping:
    """Load the Datenkompass activity mapping."""
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "bibliographic-resource.yaml")
    )
'''


@asset(group_name="datenkompass")
def datenkompass_resource_filter_mapping() -> DatenkompassFilterMapping:
    """Load the Datenkompass resource filter mapping."""
    settings = Settings.get()
    return DatenkompassFilterMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "resource_filter.yaml")
    )


@asset(group_name="datenkompass")
def datenkompass_merged_activities_by_primary_source(
    datenkompass_activity_filter_mapping: DatenkompassFilterMapping,
) -> dict[str, list[MergedActivity]]:
    """Get merged activities filtered for allowed primary source.

    We can only filter for one reference field here (will change in MX-2136).
    Therefore, we filter for the allowed primary sources per unit, since
    we can't filter for hadPrimarySources in already fetched merged activities.
    """
    filtered_primary_sources = (
        datenkompass_activity_filter_mapping.fields[1].filterRules[0].forValues
    )
    entity_type = ["MergedActivity"]
    merged_activities_by_primary_source: dict[str, list[MergedActivity]] = {}
    if filtered_primary_sources:
        for fps in filtered_primary_sources:
            primary_source_ids = get_filtered_primary_source_ids([fps])

            merged_activities_by_primary_source[fps] = cast(
                "list[MergedActivity]",
                get_merged_items(
                    entity_type=entity_type,
                    referenced_identifier=primary_source_ids,
                    reference_field="hadPrimarySource",
                ),
            )
    return merged_activities_by_primary_source


@asset(group_name="datenkompass")
def datenkompass_filtered_merged_activities(
    datenkompass_merged_activities_by_primary_source: dict[str, list[MergedActivity]],
) -> list[MergedActivity]:
    """Filter items for primary source, turn into a list, filter for organization."""
    filtered_merged_activities_by_primary_source = (
        filter_merged_items_for_primary_source(
            datenkompass_merged_activities_by_primary_source,
            "ExtractedActivity",
        )
    )
    merged_activities_filtered_for_primary_source = [
        item
        for sublist in filtered_merged_activities_by_primary_source.values()
        for item in sublist
    ]

    return filter_activities_by_organization(
        merged_activities_filtered_for_primary_source,
    )


'''@asset(group_name="datenkompass")
def datenkompass_merged_bibliographic_resources(
    datenkompass_bibliographic_resource_mapping: DatenkompassMapping,
) -> list[MergedBibliographicResource]:
    """Get merged bibliographic resources."""
    filtered_primary_sources = (
        datenkompass_bibliographic_resource_mapping.fields[0].mappingRules[0].forValues
    )
    entity_type = ["MergedBibliographicResource"]
    primary_source_ids = get_filtered_primary_source_ids(filtered_primary_sources)
    return cast(
        "list[MergedBibliographicResource]",
        get_merged_items(
            entity_type=entity_type,
            referenced_identifier=primary_source_ids,
            reference_field="hadPrimarySource",
        ),
    )
'''


@asset(group_name="datenkompass")
def datenkompass_merged_resources_by_primary_source(
    datenkompass_resource_filter_mapping: DatenkompassFilterMapping,
) -> dict[str, list[MergedResource]]:
    """Get merged resources as dictionary by primary source."""
    filtered_primary_sources = (
        datenkompass_resource_filter_mapping.fields[0].filterRules[0].forValues
    )
    entity_type = ["MergedResource"]
    merged_resources_by_primary_source: dict[str, list[MergedResource]] = {}
    if filtered_primary_sources:
        for fps in filtered_primary_sources:
            primary_source_ids = get_filtered_primary_source_ids([fps])

            merged_resources_by_primary_source[fps] = cast(
                "list[MergedResource]",
                get_merged_items(
                    entity_type=entity_type,
                    referenced_identifier=primary_source_ids,
                    reference_field="hadPrimarySource",
                ),
            )
    return merged_resources_by_primary_source


@asset(group_name="datenkompass")
def datenkompass_filtered_merged_resources_by_primary_source(
    datenkompass_merged_resources_by_primary_source: dict[str, list[MergedResource]],
) -> dict[str, list[MergedResource]]:
    """Filter the merged items for primary source "mex-editor"."""
    return filter_merged_items_for_primary_source(
        datenkompass_merged_resources_by_primary_source,
        "ExtractedResource",
    )


@asset(group_name="datenkompass")
def datenkompass_filtered_merged_resources_by_primary_source_by_unit(
    datenkompass_filtered_merged_resources_by_primary_source: dict[
        str, list[MergedResource]
    ],
    datenkompass_resource_filter_mapping: DatenkompassFilterMapping,
    datenkompass_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> dict[str, dict[str, list[MergedResource]]]:
    """Filter the merged resources by units."""
    return filter_merged_resources_by_unit(
        datenkompass_filtered_merged_resources_by_primary_source,
        datenkompass_resource_filter_mapping,
        datenkompass_merged_organizational_units_by_id,
    )


@asset(group_name="datenkompass")
def datenkompass_activities(
    datenkompass_filtered_merged_activities: list[MergedActivity],
    datenkompass_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[DatenkompassActivity]:
    """Transform activities to datenkompass items."""
    return transform_activities(
        datenkompass_filtered_merged_activities,
        datenkompass_merged_organizational_units_by_id,
    )


'''@asset(group_name="datenkompass")
def datenkompass_bibliographic_resources(
    datenkompass_merged_bibliographic_resources: list[MergedBibliographicResource],
    datenkompass_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    datenkompass_person_name_str_by_id: dict[MergedPersonIdentifier, str],
    datenkompass_bibliographic_resource_mapping: DatenkompassMapping,
) -> list[DatenkompassBibliographicResource]:
    """Transform bibliographic resources to datenkompass items."""
    return transform_bibliographic_resources(
        datenkompass_merged_bibliographic_resources,
        datenkompass_merged_organizational_units_by_id,
        datenkompass_person_name_str_by_id,
        datenkompass_bibliographic_resource_mapping,
    )
'''


@asset(group_name="datenkompass")
def datenkompass_resources_by_primary_source_by_unit(
    datenkompass_filtered_merged_resources_by_primary_source_by_unit: dict[
        str, dict[str, list[MergedResource]]
    ],
    datenkompass_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    datenkompass_merged_contact_points_by_id: dict[
        MergedContactPointIdentifier, MergedContactPoint
    ],
) -> dict[str, dict[str, list[DatenkompassResource]]]:
    """Transform resources to datenkompass items."""
    return transform_resources(
        datenkompass_filtered_merged_resources_by_primary_source_by_unit,
        datenkompass_merged_organizational_units_by_id,
        datenkompass_merged_contact_points_by_id,
    )


@asset(group_name="datenkompass")
def datenkompass_s3_xlsx_publication(
    datenkompass_activities: list[DatenkompassActivity],
    # datenkompass_bibliographic_resources: list[DatenkompassBibliographicResource],
    datenkompass_resources_by_primary_source_by_unit: dict[
        str, dict[str, list[DatenkompassResource]]
    ],
) -> None:
    """Write items to S3 xlsx."""
    s3xlsx = S3XlsxSink()
    deque(s3xlsx.load(datenkompass_activities), maxlen=0)
    """deque(
        s3xlsx.load(
            datenkompass_bibliographic_resources,
        ),
        maxlen=0,
    )"""
    for unit, inner_dict in datenkompass_resources_by_primary_source_by_unit.items():
        for primary_source, datenkompass_resources in inner_dict.items():
            deque(
                s3xlsx.load(
                    datenkompass_resources,
                    primary_source_name=primary_source,
                    unit_name=unit,
                ),
                maxlen=0,
            )


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datenkompass job in-process."""
    run_job_in_process("datenkompass")

from typing import TYPE_CHECKING, Any, cast, overload

from mex.common.organigram.helpers import find_descendants
from mex.extractors.datenkompass.extract import (
    get_extracted_item_stable_target_ids,
    get_merged_items,
)
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from mex.common.models import (
        MergedActivity,
        MergedOrganization,
        MergedOrganizationalUnit,
        MergedResource,
    )
    from mex.common.types import (
        MergedOrganizationalUnitIdentifier,
    )
    from mex.extractors.datenkompass.models.mapping import DatenkompassFilterMapping


def filter_activities_by_organization(
    datenkompass_merged_activities_by_primary_source: list[MergedActivity],
) -> list[MergedActivity]:
    """Filter the merged activities based on the mapping specifications.

    Args:
        datenkompass_merged_activities_by_primary_source: merged activities by unit.

    Returns:
        filtered list of merged activities by unit.
    """
    settings = Settings.get()
    filtered_merged_organization_ids = [
        organization.identifier
        for organization in cast(
            "list[MergedOrganization]",
            get_merged_items(
                query_string=settings.datenkompass.organization_filter,
                entity_type=["MergedOrganization"],
            ),
        )
    ]

    return [
        item
        for item in datenkompass_merged_activities_by_primary_source
        if any(
            funder in filtered_merged_organization_ids
            for funder in item.funderOrCommissioner
        )
    ]


def filter_merged_resources_by_unit(
    merged_resources_by_primary_source: dict[str, list[MergedResource]],
    resource_filter_mapping: DatenkompassFilterMapping,
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> dict[str, dict[str, list[MergedResource]]]:
    """Filter the merged resources by (unit and its childunits) in field unitInCharge.

    Args:
        merged_resources_by_primary_source: merged resources by primary source.
        resource_filter_mapping: Datenkompass resource filter mapping
        merged_organizational_units_by_id: all merged units by their id

    Returns:
        filtered list of merged resources by primary source by unit.
    """
    allowedunits_by_filterunits = {
        filter_unit: set(
            find_descendant_units(merged_organizational_units_by_id, filter_unit)
        )
        for filter_unit in (
            resource_filter_mapping.fields[1].filterRules[0].forValues or []
        )
    }
    result_resources_by_primary_source_by_unit: dict[
        str, dict[str, list[MergedResource]]
    ] = {}

    for filter_unit, allowed_units in allowedunits_by_filterunits.items():
        result_resources_by_primary_source: dict[str, list[MergedResource]] = {}
        for (
            primary_source,
            merged_resources,
        ) in merged_resources_by_primary_source.items():
            allowed_merged_resources = [
                item
                for item in merged_resources
                if allowed_units.intersection(item.unitInCharge)
            ]
            if allowed_merged_resources:
                result_resources_by_primary_source[primary_source] = (
                    allowed_merged_resources
                )
        result_resources_by_primary_source_by_unit[filter_unit] = (
            result_resources_by_primary_source
        )

    return result_resources_by_primary_source_by_unit


def find_descendant_units(
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    parent_unit_name: str,
) -> list[str]:
    """Based on filter settings find descendant unit ids.

    Args:
        merged_organizational_units_by_id: merged organizational units by identifier.
        parent_unit_name: name of the parent unit for which to find all descendants

    Returns:
        identifier of units which are descendants of the unit filter setting.
    """
    fetched_merged_organizational_units = list(
        merged_organizational_units_by_id.values()
    )
    parent_id = str(
        next(
            unit.identifier
            for unit in fetched_merged_organizational_units
            if unit.shortName and unit.shortName[0].value == parent_unit_name
        )
    )
    descendants = find_descendants(
        fetched_merged_organizational_units,
        str(parent_id),
    )
    descendants.append(parent_id)

    return descendants


@overload
def filter_merged_items_for_primary_source(
    merged_items_by_primary_source: dict[str, list[MergedResource]], entity_type: str
) -> dict[str, list[MergedResource]]: ...


@overload
def filter_merged_items_for_primary_source(
    merged_items_by_primary_source: dict[str, list[MergedActivity]], entity_type: str
) -> dict[str, list[MergedActivity]]: ...


def filter_merged_items_for_primary_source(
    merged_items_by_primary_source: dict[str, list[Any]],
    entity_type: str,
) -> dict[str, list[Any]]:
    """Filter the merged items for primary source as defined in settings.

     Special treatment for items which were created/edited in editor: filter those
     merged items out, which are referenced via stableTargetID by an extracted item,
     to keep only those merged items which consist only of rules

    Args:
        merged_items_by_primary_source: merged items dictionary by primary source.
        entity_type: entity type to of merged items

    Settings: primary source which needs to be filtered

    Returns:
        dictionary with list of filtered merged items
    """
    settings = Settings.get()

    primary_source_filter = settings.datenkompass.primary_source_filter

    if primary_source_filter in merged_items_by_primary_source:
        concerned_merged_items = merged_items_by_primary_source[primary_source_filter]
        concerned_merged_item_ids = [item.identifier for item in concerned_merged_items]
        extracted_item_stid = set(
            get_extracted_item_stable_target_ids(
                [entity_type],
                concerned_merged_item_ids,
            )
        )
        new_merged_items_by_primary_source = dict(merged_items_by_primary_source)
        new_merged_items_by_primary_source[primary_source_filter] = [
            item
            for item in concerned_merged_items
            if item.identifier not in extracted_item_stid
        ]

        return new_merged_items_by_primary_source

    return merged_items_by_primary_source

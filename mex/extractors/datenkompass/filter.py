from typing import TYPE_CHECKING, cast

from mex.common.logging import logger
from mex.common.organigram.helpers import find_descendants
from mex.extractors.datenkompass.extract import (
    get_merged_items,
)
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from mex.common.models import (
        MergedActivity,
        MergedOrganization,
        MergedOrganizationalUnit,
    )
    from mex.common.types import (
        MergedOrganizationalUnitIdentifier,
    )


def filter_activities_for_organization_and_unit(
    datenkompass_merged_activities_by_unit: dict[str, list[MergedActivity]],
    datenkompass_merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> dict[str, list[MergedActivity]]:
    """Filter the merged activities based on the mapping specifications.

    Args:
        datenkompass_merged_activities_by_unit: merged activities by unit.
        datenkompass_merged_organizational_units_by_id: units by id

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

    filtered_merged_activities_by_unit: dict[str, list[MergedActivity]] = {}

    for unit_name, activity_list in datenkompass_merged_activities_by_unit.items():
        filtered_merged_unit_ids = find_descendant_units(
            datenkompass_merged_organizational_units_by_id, unit_name
        )
        filtered_items = [
            item
            for item in activity_list
            if any(
                funder in filtered_merged_organization_ids
                for funder in item.funderOrCommissioner
            )
            and any(unit in filtered_merged_unit_ids for unit in item.responsibleUnit)
        ]
        filtered_merged_activities_by_unit[unit_name] = filtered_items

    logger.info(
        "%s items remain after filtering.",
        sum(len(v) for v in filtered_merged_activities_by_unit.values()),
    )

    return filtered_merged_activities_by_unit


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

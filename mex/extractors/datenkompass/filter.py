from collections.abc import Sequence
from typing import cast

from mex.common.logging import logger
from mex.common.models import MergedActivity, MergedOrganizationalUnit
from mex.common.organigram.helpers import find_descendants
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)

from mex.extractors.datenkompass.extract import get_merged_items, \
    get_filtered_primary_source_ids
from mex.extractors.datenkompass.models.mapping import DatenkompassFilter
from mex.extractors.settings import Settings


def filter_for_organization_and_unit(
    datenkompass_merged_activities: Sequence[MergedActivity],
) -> dict[str, list[MergedActivity]]:
    """Filter the merged activities based on the mapping specifications.

    Args:
        datenkompass_merged_activities_by_unit: merged activities by unit.

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

    filtered_merged_activities_by_unit: dict[str, list[MergedActivity]]

    for unit_name in datenkompass_merged_activities:
        filtered_merged_unit_ids = find_descendant_units(unit_name)
        filtered_items = [
            item
            for item in datenkompass_merged_activities
            if any(
                funder in filtered_merged_organization_ids
                for funder in item.funderOrCommissioner
            ) and any(
                unit in filtered_merged_unit_ids for unit in item.responsibleUnit
            )
        ]
        filtered_merged_activities_by_unit[unit_name] = filtered_items

        logger.info(
            "%s items remain after filtering for unit %s.",
            len(filtered_items),
            unit_name
        )

    return filtered_merged_activities_by_unit


def find_descendant_units(
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Based on filter settings find descendant unit ids.

    Args:
        merged_organizational_units_by_id: merged organizational units by identifier.

    Returns:
        identifier of units which are descendants of the unit filter setting.
    """
    settings = Settings.get()
    fetched_merged_organizational_units = list(
        merged_organizational_units_by_id.values()
    )
    parent_id = str(
        next(
            unit.identifier
            for unit in fetched_merged_organizational_units
            if unit.shortName
            and unit.shortName[0].value == settings.datenkompass.unit_filter
        )
    )
    descendants = find_descendants(
        fetched_merged_organizational_units,
        str(parent_id),
    )
    descendants.append(parent_id)

    return descendants

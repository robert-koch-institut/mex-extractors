from collections.abc import Sequence

from mex.common.logging import logger
from mex.common.models import MergedActivity, MergedOrganizationalUnit
from mex.common.organigram.helpers import find_descendants
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.settings import Settings


def filter_for_organization(
    extracted_merged_activities: Sequence[MergedActivity],
    relevant_merged_organization_ids: set[MergedOrganizationIdentifier],
) -> list[MergedActivity]:
    """Filter the merged activities based on the mapping specifications.

    Args:
        extracted_merged_activities: merged activities as sequence.
        relevant_merged_organization_ids: relevant merged organization identifiers.

    Returns:
        filtered list of merged activities.
    """
    filtered_items = [
        item
        for item in extracted_merged_activities
        if any(
            funder in relevant_merged_organization_ids
            for funder in item.funderOrCommissioner
        )
    ]

    logger.info("%s items remain after filtering.", len(filtered_items))

    return filtered_items


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
    settings = Settings()
    extracted_merged_organizational_units = list(
        merged_organizational_units_by_id.values()
    )
    parent_id = str(
        next(
            unit.identifier
            for unit in extracted_merged_organizational_units
            if unit.shortName
            and unit.shortName[0].value == settings.datenkompass.unit_filter
        )
    )
    descendants = find_descendants(
        extracted_merged_organizational_units,
        str(parent_id),
    )
    descendants.append(parent_id)

    return descendants

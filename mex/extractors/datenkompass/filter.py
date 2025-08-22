from collections.abc import Sequence

from mex.common.logging import logger
from mex.common.models import MergedActivity
from mex.common.types import MergedOrganizationIdentifier
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

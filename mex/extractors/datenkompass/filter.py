from collections.abc import Sequence

from mex.common.logging import logger
from mex.common.models import MergedActivity
from mex.common.types import MergedOrganizationIdentifier


def filter_for_bmg(
    extracted_merged_activities: Sequence[MergedActivity],
    extracted_merged_bmg_ids: set[MergedOrganizationIdentifier],
) -> list[MergedActivity]:
    """Filter the merged activities based on the mapping specifications.

    Args:
        extracted_merged_activities: list of merged activities as sequence.
        extracted_merged_bmg_ids: set of extracted merged bmg identifiers.

    Returns:
        filtered list of merged activities.
    """
    filtered_items = [
        item
        for item in extracted_merged_activities
        if any(
            funder in extracted_merged_bmg_ids for funder in item.funderOrCommissioner
        )
    ]

    logger.info("%s items remain after filtering.", len(filtered_items))

    return filtered_items

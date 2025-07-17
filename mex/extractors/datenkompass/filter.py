from mex.common.logging import logger
from mex.common.models import AnyMergedModel, MergedActivity
from mex.extractors.datenkompass.extract import get_merged_items


def filter_for_bmg(merged_items: list[AnyMergedModel]) -> list[MergedActivity]:
    """Filter the merged activities based on the mapping specifications.

    Args:
        merged_items: list of merged activities.

    Returns:
        filtered list of merged activities.
    """
    bmg_ids = {
        bmg.identifier for bmg in get_merged_items("BMG", ["MergedOrganization"], None)
    }

    bmg_items = [
        MergedActivity.model_validate(item)
        for item in merged_items
        if isinstance(item, MergedActivity)
        and any(funder in bmg_ids for funder in item.funderOrCommissioner)
    ]

    logger.info("%s items remain after filtering.", len(bmg_items))

    return bmg_items

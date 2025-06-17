from mex.common.models import AnyMergedModel, MergedActivity


def filter_for_bmg(merged_items: list[AnyMergedModel]) -> list[MergedActivity]:
    """Filter the merged items based on the mapping specifications."""
    bmg_id = "cvsXM2RiuPRLIyMBSJIcsk"

    return [
        MergedActivity.model_validate(item)
        for item in merged_items
        if isinstance(item, MergedActivity) and bmg_id in item.funderOrCommissioner
    ]

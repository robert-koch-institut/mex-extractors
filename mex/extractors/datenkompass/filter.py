from mex.common.models import AnyMergedModel, MergedActivity
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def filter_for_bmg(merged_items: list[AnyMergedModel]) -> list[MergedActivity]:
    """Filter the merged items based on the mapping specifications."""
    bmg_id = get_wikidata_extracted_organization_id_by_name("BMG")

    return [
        MergedActivity.model_validate(item)
        for item in merged_items
        if isinstance(item, MergedActivity) and bmg_id in item.funderOrCommissioner
    ]

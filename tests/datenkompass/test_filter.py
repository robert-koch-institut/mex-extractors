from mex.extractors.datenkompass.filter import filter_for_bmg
from tests.datenkompass.mocked_item_lists import (
    mocked_bmg,
    mocked_merged_activities,
)


def test_filter_for_bmg() -> None:
    bmg_ids = {bmg.identifier for bmg in mocked_bmg()}

    result = filter_for_bmg(
        mocked_merged_activities(),  # 3 items, one without bmg to be filtered out
        bmg_ids,
    )

    assert len(result) == 2
    assert result[0].identifier == "MergedActivityWithBMG2"
    assert result[1].identifier == "MergedActivityWithBMG1"

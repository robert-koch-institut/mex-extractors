from mex.common.models import (
    MergedActivity,
    MergedOrganization,
)
from mex.extractors.datenkompass.filter import filter_for_bmg


def test_filter_for_bmg(
    mocked_merged_activities: list[MergedActivity],
    mocked_bmg: list[MergedOrganization],
) -> None:
    bmg_ids = {bmg.identifier for bmg in mocked_bmg}
    assert len(mocked_merged_activities) == 3

    result = filter_for_bmg(
        mocked_merged_activities,  # 3 items, one to be filtered out because no bmg
        bmg_ids,
    )

    assert len(result) == 2
    assert result[0].identifier == "MergedActivityWithBMG2"
    assert result[1].identifier == "MergedActivityWithBMG1"

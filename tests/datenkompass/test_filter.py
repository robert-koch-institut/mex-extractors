from mex.common.models import (
    MergedActivity,
    MergedOrganization,
)
from mex.extractors.datenkompass.filter import filter_for_bmg
from mex.extractors.datenkompass.filter import (
    filter_for_organization,
)


def test_filter_for_organization(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organization: list[MergedOrganization],
) -> None:
    organization_ids = {
        organization.identifier for organization in mocked_merged_organization
    }
    assert len(mocked_merged_activities) == 3

    result = filter_for_organization(
        mocked_merged_activities,  # 3 items, one to be filtered out because wrong organization
        organization_ids,
    )

    assert len(result) == 2
    assert result[0].identifier == "MergedActivityWithBMG2"
    assert result[1].identifier == "MergedActivityWithBMG1"
    assert result[0].identifier == "MergedActivityWithORG2"
    assert result[1].identifier == "MergedActivityWithORG1"

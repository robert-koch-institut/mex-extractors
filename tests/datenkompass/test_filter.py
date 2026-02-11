from mex.common.models import (
    MergedActivity,
    MergedOrganization,
    MergedOrganizationalUnit,
)
from mex.extractors.datenkompass.filter import (
    filter_for_organization,
    find_descendant_units,
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
    assert result[0].identifier == "MergedActivityWithORG2"
    assert result[1].identifier == "MergedActivityWithORG1"


def test_find_descendant_units(
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    mocked_merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }

    result = find_descendant_units(mocked_merged_organizational_units_by_id)

    assert result == ["IdentifierOrgUnitZB", "IdentifierOrgUnitEG"]

import pytest

from mex.common.models import (
    MergedActivity,
    MergedOrganizationalUnit,
)
from mex.extractors.datenkompass.filter import (
    filter_activities_for_organization_and_unit,
    find_descendant_units,
)


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_filter_activities_for_organization_and_unit(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    mocked_activities_by_unit = {
        "PRNT": mocked_merged_activities,
        "FG 99": mocked_merged_activities,
    }
    mocked_units_by_ids = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }

    result = filter_activities_for_organization_and_unit(
        mocked_activities_by_unit,  # 3 items, one to be filtered out because wrong organization
        mocked_units_by_ids,
    )

    assert len(result["PRNT"]) == 2
    assert len(result["FG 99"]) == 1
    assert result["PRNT"][0].identifier == "MergedActivityWithORG2"
    assert result["PRNT"][1].identifier == "MergedActivityWithORG1"
    assert result["FG 99"][0].identifier == "MergedActivityWithORG2"


def test_find_descendant_units(
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    mocked_units_by_ids = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }

    result = find_descendant_units(mocked_units_by_ids, "PRNT")

    assert result == ["IdentifierUnitC1", "IdentifierUnitPRNT"]

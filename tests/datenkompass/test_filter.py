import pytest

from mex.common.models import (
    MergedActivity,
    MergedOrganizationalUnit,
    MergedResource,
)
from mex.extractors.datenkompass.filter import (
    filter_activities_for_organization_and_unit,
    filter_merged_resources_by_unit,
    find_descendant_units,
)
from mex.extractors.datenkompass.models.mapping import DatenkompassFilterMapping
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


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


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_filter_merged_resources_by_unit(
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    mocked_resources_by_primary_source = {
        "relevant primary source": mocked_merged_resource,
    }
    mocked_units_by_ids = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    settings = Settings.get()
    mocked_resource_filter_mapping = DatenkompassFilterMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "resource_filter.yaml")
    )

    result = filter_merged_resources_by_unit(
        mocked_resources_by_primary_source,
        mocked_resource_filter_mapping,
        mocked_units_by_ids,
    )

    # 4 items before filtering. One to be filtered out because wrong unit
    assert len(mocked_resources_by_primary_source["relevant primary source"]) == 4
    assert len(result["PRNT"]) == 1
    assert len(result["FG 99"]) == 1
    assert len(result["PRNT"]["relevant primary source"]) == 1
    assert len(result["FG 99"]["relevant primary source"]) == 2
    assert (
        result["PRNT"]["relevant primary source"][0].identifier
        == "IdentifierC1Resource"
    )
    assert (
        result["FG 99"]["relevant primary source"][0].identifier
        == "IdentifierFG99Resource"
    )


def test_find_descendant_units(
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    mocked_units_by_ids = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }

    result = find_descendant_units(mocked_units_by_ids, "PRNT")

    assert result == ["IdentifierUnitC1", "IdentifierUnitPRNT"]

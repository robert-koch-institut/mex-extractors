from typing import TYPE_CHECKING

import pytest

from mex.extractors.datenkompass.filter import (
    filter_activities_by_organization,
    filter_merged_items_for_primary_source,
    filter_merged_resources_by_unit,
    find_descendant_units,
)
from mex.extractors.datenkompass.models.mapping import DatenkompassFilterMapping
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from mex.common.models import (
        MergedActivity,
        MergedOrganizationalUnit,
        MergedResource,
    )


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_filter_activities_by_organization(
    mocked_merged_activities: list[MergedActivity],
) -> None:
    result = filter_activities_by_organization(
        mocked_merged_activities,  # 3 items, one to be filtered out
    )

    assert len(result) == 2
    assert result[0].identifier == "MergedActivityWithORG2"
    assert result[1].identifier == "MergedActivityWithORG1"


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


@pytest.mark.usefixtures("mocked_backend_datenkompass", "mocked_provider")
def test_filter_filter_merged_items_for_primary_source(
    mocked_merged_resource: list[MergedResource],
) -> None:
    mocked_resources_by_primary_source = {
        "relevant primary source": mocked_merged_resource,
        "filter primary source": mocked_merged_resource,
    }

    result = filter_merged_items_for_primary_source(
        mocked_resources_by_primary_source,
        "ExtractedResource",
    )

    assert len(result["relevant primary source"]) == 4
    assert len(result["filter primary source"]) == 3
    assert "IdMergedWithExtracted" in [
        item.identifier for item in result["relevant primary source"]
    ]
    assert "IdMergedWithExtracted" not in [
        item.identifier for item in result["filter primary source"]
    ]

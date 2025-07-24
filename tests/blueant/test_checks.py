import pytest

from dagster import build_asset_check_context
from mex.extractors.blueant.checks import check_yaml_rules_exist


@pytest.mark.usefixtures("mock_get_rule_details_from_model")
def test_load_rule_details() -> None:
    context = build_asset_check_context()
    result = check_yaml_rules_exist(context)

    assert result.passed
    assert result.metadata["num_rules"].value == 2
    rule_types = result.metadata["rule_types"].data
    assert rule_types == ["x_items_more_than", "x_items_less_than"]

@pytest.mark.usefixtures("mock_load_asset_check_from_settings", "mock_settings")
def test_check_yaml_rules_exist() -> None:
    context = build_asset_check_context()
    result = check_yaml_rules_exist(context)

    assert result.passed is True
    assert result.metadata["num_rules"].value == 2
    rule_types = result.metadata["rule_types"].data
    assert "x_items_more_than" in rule_types

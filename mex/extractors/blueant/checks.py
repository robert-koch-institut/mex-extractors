from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetKey, asset_check
from mex.extractors.pipeline.checks.main import (
    check_x_items_more_passed,
    check_yaml_path,
    get_rule,
)


@asset_check(asset="extracted_blueant_activities", blocking=True)
def check_yaml_rules_exist() -> AssetCheckResult:
    """Check if any rules exist for blueant activity with valid threshold."""
    extractor = "blueant"
    entity_type = "activity"
    yaml_exists = check_yaml_path(extractor, entity_type)
    if yaml_exists:
        model = get_rule("x_items_more_than", extractor, entity_type)
        value = model["value"]
        passed = value is not None and value > 0
    else:
        passed = True
    return AssetCheckResult(passed=passed)


@asset_check(
    asset="extracted_blueant_activities",
    additional_deps=["check_yaml_rules_exist"],
    blocking=True,
)
def check_x_items_more_than(
    context: AssetCheckExecutionContext, extracted_blueant_activities: int
) -> AssetCheckResult:
    """Check that latest count is not more than threshold + historical count."""
    asset_key = AssetKey(["extracted_blueant_activities"])
    passed = check_x_items_more_passed(
        context, asset_key, "blueant", "activity", extracted_blueant_activities
    )
    return AssetCheckResult(passed=passed)

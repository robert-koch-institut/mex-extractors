from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetKey, asset_check
from mex.extractors.pipeline.checks.main import (
    check_x_items_more_passed,
    check_yaml_path,
    get_rule,
)


@asset_check(asset="extracted_blueant_activities", blocking=True)
def check_yaml_rules_exist(context) -> AssetCheckResult:
    """Check if any asset check rules exist for blueant activity."""
    extractor= "blueant"
    entity_type = "activity"
    yaml_exists = check_yaml_path(extractor, entity_type)
    if yaml_exists:
        model = get_rule("x_items_more_than", extractor, entity_type)
        value = model["value"]
        context.log.info(f"CHECK MODEL: {model}, VALUE: {value}")
        passed = value is not None and value > 0
    else:
        passed = True
    return AssetCheckResult(
        passed=passed
    )



@asset_check(asset="extracted_blueant_activities",  additional_deps=["check_yaml_rules_exist"], blocking=True)
def check_x_items_more_than(
    context: AssetCheckExecutionContext, extracted_blueant_activities: int
) -> AssetCheckResult:
    """Check that latest item count is not more than threshold over historical baseline."""
    asset_key = AssetKey(["extracted_blueant_activities"])
    passed = check_x_items_more_passed(
        context, asset_key, "blueant", "activity", extracted_blueant_activities
    )
    return AssetCheckResult(
        passed=passed
    )

from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetKey, asset_check

from mex.extractors.pipeline.checks.main import (
    check_x_items_more_passed,
    check_yaml_path,
    get_rule,
)


@asset_check(asset="extracted_endnote_bibliographic_resources", blocking=True)
def check_yaml_rules_exist() -> AssetCheckResult:
    """Check if any rules exist for endnote with valid threshold."""
    extractor = "endnote"
    entity_type = "bibliographic-resource"
    yaml_exists = check_yaml_path(extractor, entity_type)
    if yaml_exists:
        model = get_rule("x_items_more_than", extractor, entity_type)
        value = model["value"]
        passed = value is not None and value > 0
    else:
        passed = True
    return AssetCheckResult(passed=passed)


@asset_check(
    asset="extracted_endnote_bibliographic_resources",
    additional_deps=["check_yaml_rules_exist"],
    blocking=True,
)
def check_x_items_more_than(
    context: AssetCheckExecutionContext, extracted_endnote_bibliographic_resources: int
) -> AssetCheckResult:
    """Check that latest count is not more than threshold + historical count."""
    asset_key = AssetKey(["extracted_endnote_bibliographic_resources"])
    passed = check_x_items_more_passed(
        context,
        asset_key,
        "endnote",
        "bibliographic-resource",
        extracted_endnote_bibliographic_resources,
    )
    return AssetCheckResult(passed=passed)

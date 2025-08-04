from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetKey, asset_check
from mex.extractors.pipeline.checks.main import (
    check_x_items_more_passed,
    check_yaml_path,
    get_rule,
)

# @asset_check(asset="extracted_endnote_bibliographic_resources", blocking=True)
# def check_yaml_rules_exist(context: AssetCheckExecutionContext) -> AssetCheckResult:
#     """Check if any asset check rules exist for endnote bibliographic-resource."""
#     check_model = load_asset_check_from_settings( "endnote", "bibliographic-resource")
#     num_rules = len(check_model.rules)
#     return AssetCheckResult(
#         passed=num_rules > 0,
#         metadata={
#             "num_rules": num_rules,
#             "rule_types": [r.fail_if for r in check_model.rules],
#         },
#     )


# @asset_check(asset="extracted_endnote_bibliographic_resources", blocking=True)
# def check_x_items_more_than(
#     context: AssetCheckExecutionContext, extracted_endnote_bibliographic_resources: int
# ) -> AssetCheckResult:
#     """Check that latest item count is not more than threshold over historical baseline."""
#     asset_key = AssetKey(["extracted_endnote_bibliographic_resources"])
#     passed = check_x_items_more_passed(
#         context, asset_key, "endnote", "bibliographic-resource", extracted_endnote_bibliographic_resources
#     )
#     # TODO if no historic then just pass. Error only when extracting is higher than historic.
#     return AssetCheckResult(
#         passed=passed
#     )


@asset_check(asset="extracted_endnote_bibliographic_resources", blocking=True)
def check_yaml_rules_exist(context) -> AssetCheckResult:
    """Check if any asset check rules exist for endnote bibliographic-resource."""
    extractor = "endnote"
    entity_type = "bibliographic-resource"
    yaml_exists = check_yaml_path(extractor, entity_type)
    if yaml_exists:
        model = get_rule("x_items_more_than", extractor, entity_type)
        value = model["value"]
        context.log.info(f"CHECK MODEL: {model}, VALUE: {value}")
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
    """Check that latest item count is not more than threshold over historical baseline."""
    asset_key = AssetKey(["extracted_endnote_bibliographic_resources"])
    passed = check_x_items_more_passed(
        context,
        asset_key,
        "endnote",
        "bibliographic-resource",
        extracted_endnote_bibliographic_resources,
    )
    return AssetCheckResult(passed=passed)

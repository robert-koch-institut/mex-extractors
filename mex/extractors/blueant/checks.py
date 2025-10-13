from collections.abc import Iterable

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    DagsterEventType,
    EventRecordsFilter,
    asset_check,
    multi_asset_check,
)

from mex.extractors.pipeline.checks.main import (
    check_x_items_less_passed,
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
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)


@asset_check(
    asset="extracted_blueant_activities",
    additional_deps=["check_yaml_rules_exist"],
    blocking=True,
)
def check_x_items_more_than(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Check that latest count is not more than threshold + historical count."""
    asset_key = AssetKey(["extracted_blueant_activities"])
    passed = check_x_items_more_passed(context, asset_key, "blueant", "activity")
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)


@asset_check(
    asset="extracted_blueant_activities",
    additional_deps=["check_yaml_rules_exist"],
    blocking=True,
)
def check_x_items_less_than(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Dagster asset check for 'x_items_less_than' rule."""
    asset_key = AssetKey(["extracted_blueant_activities"])
    passed = check_x_items_less_passed(context, asset_key, "blueant", "activity")
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)


@multi_asset_check(
    specs=[
        AssetCheckSpec(
            name="blueant_yaml_rules_exist", asset="extracted_blueant_activities"
        ),
        AssetCheckSpec(
            name="blueant_x_items_more_than", asset="extracted_blueant_activities"
        ),
        AssetCheckSpec(
            name="blueant_x_items_less_than", asset="extracted_blueant_activities"
        ),
    ],
)
def blueant_checks(context: AssetCheckExecutionContext) -> Iterable[AssetCheckResult]:
    """Run all checks."""
    asset_key = list(context.selected_asset_check_keys)[0].asset_key
    extractor = context.job_def.metadata["group_name"].text

    # ----------------- old way with asset decorator
    # entity_type = context.repository_def.assets_defs_by_key[asset_key].metadata_by_key[
    #     asset_key
    # ]["entity_type"]

    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_OBSERVATION, asset_key=asset_key
        ),
        limit=1,
    )
    entity_type = records[0].asset_observation.metadata.get("entity_type").text
    # yaml check
    yaml_exists = check_yaml_path(extractor, entity_type)
    if yaml_exists:
        model = get_rule("x_items_more_than", extractor, entity_type)
        value = model["value"]
        yaml_passed = value is not None and value > 0
    else:
        yaml_passed = True
    yield AssetCheckResult(
        check_name=f"{extractor}_yaml_rules_exist",
        passed=yaml_passed,
        severity=AssetCheckSeverity.ERROR,
        asset_key=asset_key,
    )

    # items_more_than
    more_than_passed = check_x_items_more_passed(
        context,
        asset_key,
        extractor,
        entity_type,
    )
    yield AssetCheckResult(
        check_name=f"{extractor}_x_items_more_than",
        passed=more_than_passed,
        severity=AssetCheckSeverity.ERROR,
        asset_key=asset_key,
    )

    # x_items_less_than
    less_than_passed = check_x_items_less_passed(
        context,
        asset_key,
        extractor,
        entity_type,
    )
    yield AssetCheckResult(
        check_name=f"{extractor}_x_items_less_than",
        passed=less_than_passed,
        severity=AssetCheckSeverity.ERROR,
        asset_key=asset_key,
    )

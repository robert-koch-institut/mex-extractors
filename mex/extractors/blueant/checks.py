from collections.abc import Iterable
from importlib import import_module

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    AssetsDefinition,
    DagsterEventType,
    EventRecordsFilter,
    asset_check,
    load_assets_from_package_module,
    multi_asset_check,
)

from mex.common.logging import logger
from mex.extractors.pipeline.base import load_job_definitions
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

def check_asset_dependencies(asset_def: AssetsDefinition):
#    dep_keys = asset_def.dependency_keys

    asset_deps = asset_def.asset_deps

    for asset_key in asset_def.keys:
        deps_for_asset = asset_deps.get(asset_key, set())
        if deps_for_asset:
            logger.info(msg=f"{asset_key} depends on: {deps_for_asset}")
        else:
            logger.info(msg=f"{asset_key} has no dependencies")

def create_checks_specs(extractor_name: str) -> list[AssetCheckSpec]:
    """Creates dynmically specs for asset checks for given extractor."""
    module = import_module("mex.extractors.blueant")
    assets = load_assets_from_package_module(module)

    specs: list[AssetCheckSpec] = []
    for asset_def in assets:
        for key, group_name in asset_def.group_names_by_key.items():
            if group_name != extractor_name:
                continue

            deps_for_asset = asset_def.asset_deps.get(key, set())
            has_upstream = bool(deps_for_asset)
            blocking = not has_upstream
            if blocking:
                logger.info(msg=f"################DEPS############ {key} has no dependencies")

            specs.extend([
                AssetCheckSpec(
                    name=f"{extractor_name}_yaml_rules_exist",
                    asset=key,
                    blocking=True,
                ),
                AssetCheckSpec(
                    name=f"{extractor_name}_x_items_more_than",
                    asset=key,
                    blocking=True,
                ),
                AssetCheckSpec(
                    name=f"{extractor_name}_x_items_less_than",
                    asset=key,
                    blocking=True,
                ),
            ])
    return specs

# @multi_asset_check(specs=create_checks_specs("blueant"))
def blueant_checks(context: AssetCheckExecutionContext) -> Iterable[AssetCheckResult]:
    """Run all checks."""

    # get all assets by current job
    selected_keys = context.selected_asset_check_keys
    asset_keys = [check_key.asset_key for check_key in selected_keys]

    # current asset_key
    # asset_key = list(context.selected_asset_check_keys)[0].asset_key
    extractor = context.job_def.metadata["group_name"].text

    # ----------------- old way with asset decorator
    # entity_type = context.repository_def.assets_defs_by_key[asset_key].metadata_by_key[
    #     asset_key
    # ]["entity_type"]
    for asset_key in asset_keys:
        records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION, asset_key=asset_key
            ),
            limit=1,
        )
        if not records:
            yield AssetCheckResult(
                check_name=f"{asset_key}_no_observation",
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                asset_key=asset_key,
            )
            continue
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

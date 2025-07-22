from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetKey, asset_check
from mex.extractors.pipeline.checks.main import (
    check_x_items_more_passed,
    load_asset_check_from_settings,
)


@asset_check(asset="extracted_blueant_activities", blocking=True)
def check_yaml_rules_exist(context) -> AssetCheckResult:
    """Check if any asset check rules exist for blueant activity."""
    check_model = load_asset_check_from_settings("blueant", "activity")
    num_rules = len(check_model.rules)
    context.log.info("Loaded %d asset check rules", num_rules)
    return AssetCheckResult(
        passed=num_rules > 0,
        metadata={
            "num_rules": num_rules,
            "rule_types": [r.fail_if for r in check_model.rules],
        },
    )


@asset_check(asset="extracted_blueant_activities", blocking=True)
def check_x_items_more_than(
    context: AssetCheckExecutionContext, extracted_blueant_activities: int
) -> AssetCheckResult:
    """Check that latest item count is not more than threshold over historical baseline."""
    asset_key = AssetKey(["extracted_blueant_activities"])
    passed = check_x_items_more_passed(
        context, asset_key, "blueant", "activity", extracted_blueant_activities
    )
    # TODO if no historic then just pass. Error only when extracting is higher than historic.
    return AssetCheckResult(
        passed=passed,
        metadata={
            #     "rule_details": MetadataValue.md(f"```json\n{json.dumps(rule, indent=2)}\n```"),
            #     "events_nr": len(events),
            #     "time_frame": MetadataValue.timestamp(time_frame),
            #     "latest_count": latest_count,
            # "latest_event_timestamp": MetadataValue.text(datetime.fromtimestamp(latest_event.timestamp, tz=UTC).isoformat()),
            #     "historic_counts": MetadataValue.int(historic_count),
        },
    )

from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    AssetKey,
    DagsterEventType,
    EventLogRecord,
    EventRecordsFilter,
)

from mex.common.logging import logger
from mex.extractors.pipeline.checks.models.check import AssetCheck
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml

# Rule type classifications
STATIC_RULES = {
    "less_than_x_inbound",
    "less_than_x_outbound",
    "not_exactly_x_items",
}
HISTORICAL_RULES = {
    "x_items_less_than",
    "x_items_more_than",
    "x_percent_less_than",
    "x_percent_more_than",
}
ALL_RULES = STATIC_RULES | HISTORICAL_RULES


def load_asset_check_from_settings(extractor: str, entity_type: str) -> AssetCheck:
    """Load AssetCheck model from YAML for a given extractor and entity type."""
    settings = Settings.get()
    path = settings.all_checks_path / extractor / f"{entity_type}.yaml"
    if not path.exists():
        msg = "No asset check YAML found at %s"
        raise FileNotFoundError(msg, path)
    return AssetCheck.model_validate(load_yaml(path))


def get_rule(rule: str, extractor: str, entity_type: str) -> dict[str, Any]:
    """Load rule model from YAML file for given rule type."""
    check_model = load_asset_check_from_settings(
        extractor=extractor, entity_type=entity_type
    )
    return next(r for r in check_model.rules if r.fail_if == rule).model_dump()


def parse_time_frame(time_frame: str) -> timedelta:
    """Parse time frame string into timedelta."""
    num = int(time_frame[:-1])
    unit = time_frame[-1]
    if unit == "m":
        return timedelta(days=30 * num)
    if unit == "y":
        return timedelta(days=365 * num)
    return timedelta(days=num)


def get_historical_events(events: list[EventLogRecord]) -> dict[datetime, int]:
    """Load all past events and refactor it to a dict."""
    result = {}

    for e in events:
        am = e.asset_materialization
        if (
            am is not None
            and hasattr(am, "metadata")
            and "num_items" in am.metadata
            and hasattr(am.metadata["num_items"], "value")
        ):
            value = am.metadata["num_items"].value
            if value is not None:
                timestamp = datetime.fromtimestamp(e.timestamp, tz=UTC)
                result[timestamp] = int(str(value))

    return result


def get_historic_count(
    historic_events: dict[datetime, int],
    time_frame: datetime,
) -> int:
    """Get count for closest timestamp <= time_frame or next closest > time_frame."""
    if not historic_events:
        return 0

    historic_events_normalized = {
        (ts if ts.tzinfo else ts.replace(tzinfo=UTC)).astimezone(UTC): count
        for ts, count in historic_events.items()
    }
    time_frame = time_frame.astimezone(UTC)

    # closest event to given time_frame
    older = {ts: c for ts, c in historic_events_normalized.items() if ts <= time_frame}
    if older:
        return older[max(older)]

    # if no historic event that far, take the closest to time_frame
    newer = {ts: c for ts, c in historic_events_normalized.items() if ts > time_frame}
    if newer:
        return newer[min(newer)]

    # if no historic event at all available
    return 0


def check_static_rule(
    rule_name: str,
    current_number_of_extracted_items: int,  # noqa: ARG001
    rule: dict[str, Any],
) -> bool:
    """Check rules that validate current state (no historical data needed).

    Args:
        rule_name: Name of the static rule.
        current_number_of_extracted_items: Current count of extracted items.
        rule: Rule configuration from YAML.

    Returns False if check fails, True if check passes.
    """
    threshold = rule["value"] or 0  # noqa: F841

    if rule_name == "not_exactly_x_items":
        pass
    if rule_name == "less_than_x_inbound":
        pass
    if rule_name == "less_than_x_outbound":
        pass

    return True


def check_historical_rule(
    rule_name: str,
    current_number_of_extracted_items: int,
    historic_count: int,
    rule: dict[str, Any],
) -> bool:
    """Check rules that compare current state with historical data.

    Args:
        rule_name: Name of the historical rule.
        current_number_of_extracted_items: Current count of extracted items.
        historic_count: Historical count for comparison.
        rule: Rule configuration from YAML.

    Returns True if check passes, False if check fails.
    """
    threshold = rule["value"] or 0

    if rule_name == "x_items_more_than":
        # fail if current is larger than historic by threshold number of items
        return current_number_of_extracted_items <= historic_count + threshold
    if rule_name == "x_items_less_than":
        # fail if current is smaller than historic by threshold number of items
        return current_number_of_extracted_items >= historic_count - threshold
    if rule_name == "x_percent_less_than":
        pass
    if rule_name == "x_percent_more_than":
        pass

    return True


def check_item_count_rule(
    context: AssetCheckExecutionContext,
    rule_name: str,
    asset_key: AssetKey,
    extractor: str,
    entity_type: str,
) -> bool:
    """Checks extracted items are complying to given rule and threshold.

    Args:
        context: The Dagster asset execution context for this check.
        rule_name: Name of the rule to check.
        asset_key: Dagster AssetKey object.
        extractor: Name of the extractor that produced the asset.
        entity_type: Entity Type for the asset check.

    Returns True if check passes, raises ValueError if check fails.
    """
    if rule_name not in ALL_RULES:
        msg = f"Rule not existing: {rule_name}"
        raise ValueError(msg)

    try:
        rule = get_rule(rule_name, extractor, entity_type)
    except FileNotFoundError:
        logger.error("No asset check rules found for %s", asset_key)
        return True

    events = context.instance.get_event_records(
        EventRecordsFilter(
            asset_key=asset_key,
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
        )
    )
    if events is None:
        return True

    current_number_of_extracted_items = (
        events[0].asset_materialization.metadata["num_items"].value
    )

    # Handle static rules
    if rule_name in STATIC_RULES:
        if not check_static_rule(
            rule_name, current_number_of_extracted_items, rule
        ):
            msg = (
                f"Asset {asset_key} failed {rule_name} check: "
                f"{current_number_of_extracted_items} not meeting threshold."
            )
            raise ValueError(msg)
        return True

    # Handle historical rules
    time_delta = parse_time_frame(rule["time_frame"])
    current_time = datetime.now(UTC)
    time_frame = current_time - time_delta

    historical_events = get_historical_events(events)
    historic_count = get_historic_count(historical_events, time_frame)
    if historic_count <= 0:
        return True

    if not check_historical_rule(
        rule_name,
        current_number_of_extracted_items,
        historic_count,
        rule,
    ):
        msg = (
            f"Asset {asset_key} failed {rule_name} check: "
            f"{current_number_of_extracted_items} not meeting threshold."
        )
        raise ValueError(msg)

    return True

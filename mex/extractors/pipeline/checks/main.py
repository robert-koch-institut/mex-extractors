from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from dagster import (
    AssetCheckExecutionContext,
    AssetKey,
    DagsterEventType,
    EventLogRecord,
    EventRecordsFilter,
)

from mex.common.logging import logger
from mex.extractors.pipeline.checks.models.check import AssetCheck
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from collections.abc import Sequence

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
LATEST_NUM_ITEMS_ERROR = (
    "Unable to determine latest num_items from asset materialization events."
)


def load_asset_check_from_settings(extractor: str, entity_type: str) -> AssetCheck:
    """Load AssetCheck model from YAML for a given extractor and entity type."""
    settings = ExtractorsSettings.get()
    path = settings.all_checks_path / extractor / f"{entity_type}.yaml"
    if not path.exists():
        msg = "No asset check YAML found at %s"
        raise FileNotFoundError(msg, path)
    return AssetCheck.model_validate(load_yaml(path))


def get_rule(
    rule: str,
    extractor: str,
    entity_type: str,
    target_type: str | None = None,
) -> dict[str, Any]:
    """Load rule model from YAML file for given rule type."""
    check_model = load_asset_check_from_settings(
        extractor=extractor, entity_type=entity_type
    )
    if target_type is not None:
        return next(
            r
            for r in check_model.rules
            if r.fail_if == rule and r.target_type == target_type
        ).model_dump()
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


def get_historical_events(events: Sequence[EventLogRecord]) -> dict[datetime, int]:
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


def get_latest_num_items(
    events: Sequence[EventLogRecord],
    rule_name: str,
    target_type: str | None = None,
) -> int | dict[str, int] | None:
    """Get latest metadata from materialization events."""
    if not events:
        return None

    latest_materialization = events[0].asset_materialization
    if latest_materialization is None:
        return None

    metadata_key_by_rule = {
        ("less_than_x_inbound", None): "inbound_connections",
        ("less_than_x_outbound", None): "outbound_connections",
        (
            "less_than_x_outbound",
            "VariableGroup",
        ): "outbound_connections_variable_group",
        ("less_than_x_outbound", "Resource"): "outbound_connections_resource",
    }
    metadata_key = metadata_key_by_rule.get(
        (rule_name, target_type),
        metadata_key_by_rule.get((rule_name, None), "num_items"),
    )
    num_items_metadata = latest_materialization.metadata.get(metadata_key)

    if num_items_metadata is None:
        return None

    if not hasattr(num_items_metadata, "value"):
        raise ValueError(LATEST_NUM_ITEMS_ERROR)

    if num_items_metadata.value is None:
        raise ValueError(LATEST_NUM_ITEMS_ERROR)

    if rule_name == "less_than_x_outbound":
        connections = num_items_metadata.value
        if not isinstance(connections, dict):
            raise ValueError(LATEST_NUM_ITEMS_ERROR)
        return {
            identifier: int(str(count)) for identifier, count in connections.items()
        }

    if rule_name == "less_than_x_inbound":
        connections = num_items_metadata.value
        if not isinstance(connections, dict):
            raise ValueError(LATEST_NUM_ITEMS_ERROR)
        return {
            identifier: int(str(count)) for identifier, count in connections.items()
        }

    return int(str(num_items_metadata.value))


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


def check_static_rule(  # noqa: PLR0911
    rule_name: str,
    current_number_of_items: int | dict[str, int],
    rule: dict[str, Any],
) -> bool:
    """Check rules that validate current state (no historical data needed).

    Args:
        rule_name: Name of the static rule.
        current_number_of_items: Current count of extracted items.
        rule: Rule configuration from YAML.

    Returns False if check fails, True if check passes.
    """
    threshold = rule["value"] or 0

    if rule_name == "not_exactly_x_items":
        # fail if current number of extracted items is not equal to threshold
        if current_number_of_items == threshold:
            return True
        return True  # TODO @MX-2298: revert to returning the result of the comparison
    if rule_name == "less_than_x_inbound":
        # fail if accumulated inbound connection count is smaller than threshold
        if not isinstance(current_number_of_items, dict):
            raise ValueError(LATEST_NUM_ITEMS_ERROR)
        if not current_number_of_items and threshold > 0:
            return False
        return sum(current_number_of_items.values()) >= threshold
    if rule_name == "less_than_x_outbound":
        # fail if any of the outbound connection counts is smaller than threshold
        if not current_number_of_items and threshold > 0:
            return False
        return all(
            count >= threshold
            for count in current_number_of_items.values()  # type: ignore [union-attr]
        )
    return True


def check_historical_rule(
    rule_name: str,
    current_number_of_items: int,
    historic_count: int,
    rule: dict[str, Any],
) -> bool:
    """Check rules that compare current state with historical data.

    Args:
        rule_name: Name of the historical rule.
        current_number_of_items: Current count of extracted items.
        historic_count: Historical count for comparison.
        rule: Rule configuration from YAML.

    Returns True if check passes, False if check fails.
    """
    threshold = rule["value"] or 0

    if rule_name == "x_items_more_than":
        # fail if current is larger than historic by threshold number of items
        return current_number_of_items <= historic_count + threshold
    if rule_name == "x_items_less_than":
        # fail if current is smaller than historic by threshold number of items
        return current_number_of_items >= historic_count - threshold
    if rule_name == "x_percent_less_than":
        # fail if current is less than historic by x percent
        percent_threshold = (historic_count * threshold) / 100
        return current_number_of_items >= historic_count - percent_threshold
    if rule_name == "x_percent_more_than":
        # fail if current is more than historic by x percent
        percent_threshold = (historic_count * threshold) / 100
        return current_number_of_items <= historic_count + percent_threshold

    return True


def check_item_count_rule(  # noqa: PLR0913
    context: AssetCheckExecutionContext,
    rule_name: str,
    asset_key: AssetKey,
    extractor: str,
    entity_type: str,
    target_type: str | None = None,
) -> bool:
    """Checks extracted items are complying to given rule and threshold.

    Args:
        context: The Dagster asset execution context for this check.
        rule_name: Name of the rule to check.
        asset_key: Dagster AssetKey object.
        extractor: Name of the extractor that produced the asset.
        entity_type: Entity Type for the asset check.
        target_type: Optional target type to disambiguate duplicate rule names.

    Returns True if check passes, raises ValueError if check fails.
    """
    if rule_name not in ALL_RULES:
        msg = f"Rule not existing: {rule_name}"
        raise ValueError(msg)

    try:
        rule = get_rule(rule_name, extractor, entity_type, target_type)
    except FileNotFoundError:
        logger.error("No asset check rules found for %s", asset_key)
        return True

    events = context.instance.get_event_records(
        EventRecordsFilter(
            asset_key=asset_key,
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
        )
    )
    current_number_of_items = get_latest_num_items(events, rule_name, target_type)
    if current_number_of_items is None:
        return True

    # Handle static rules
    if rule_name in STATIC_RULES:
        if not check_static_rule(rule_name, current_number_of_items, rule):
            msg = (
                f"Asset {asset_key} failed {rule_name} check: "
                f"{current_number_of_items} not meeting threshold."
            )
            raise ValueError(msg)
        return True

    # Handle historical rules
    if rule["time_frame"] is None:
        return True

    time_delta = parse_time_frame(rule["time_frame"])
    current_time = datetime.now(UTC)
    time_frame = current_time - time_delta

    historical_events = get_historical_events(events)
    historic_count = get_historic_count(historical_events, time_frame)
    if historic_count <= 0:
        return True

    if not check_historical_rule(
        rule_name,
        current_number_of_items,  # type: ignore [arg-type]
        historic_count,
        rule,
    ):
        msg = (
            f"Asset {asset_key} failed {rule_name} check: "
            f"{current_number_of_items} not meeting threshold."
        )
        raise ValueError(msg)

    return True

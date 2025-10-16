from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    AssetKey,
    DagsterEventType,
    EventLogRecord,
    EventRecordsFilter,
)

from mex.common.transform import dromedary_to_kebab
from mex.extractors.pipeline.checks.models.check import AssetCheck
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


def check_yaml_path(extractor: str, entity_type: str) -> bool:
    """Checks if there are rules given the extractor and entityType.

    Returns a bool.
    """
    entity_type = dromedary_to_kebab(entity_type)
    settings = Settings.get()
    path = settings.all_checks_path / extractor / f"{entity_type}.yaml"
    return path.exists()


def load_asset_check_from_settings(extractor: str, entity_type: str) -> AssetCheck:
    """Load AssetCheck model from YAML for a given extractor and entity type."""
    settings = Settings.get()
    path = settings.all_checks_path / extractor / f"{entity_type}.yaml"
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


def check_x_items_more_passed(
    context: AssetCheckExecutionContext,
    asset_key: AssetKey,
    extractor: str,
    entity_type: str,
) -> bool:
    """Checks whether latest extracted items nr is exceeding the rule threshold.

    Returns bool to AssetCheck.
    """
    yaml_exists = check_yaml_path(extractor, entity_type)

    if not yaml_exists:
        return True

    rule = get_rule("x_items_more_than", extractor, entity_type)
    time_delta = parse_time_frame(rule["time_frame"])
    current_time = datetime.now(UTC)
    time_frame = current_time - time_delta

    events = context.instance.get_event_records(
        EventRecordsFilter(
            asset_key=asset_key, event_type=DagsterEventType.ASSET_MATERIALIZATION
        )
    )
    if events is None:
        return True

    actual_count = events[0].asset_materialization.metadata["num_items"].value
    historical_events = get_historical_events(events)
    historic_count = get_historic_count(historical_events, time_frame)
    return actual_count <= (historic_count if historic_count > 0 else actual_count) + (
        rule["value"] or 0
    )


def check_x_items_less_passed(
    context: AssetCheckExecutionContext,
    asset_key: AssetKey,
    extractor: str,
    entity_type: str,
) -> bool:
    """Checks whether latest extracted items number is lower than historical count.

    Returns bool to AssetCheck.
    """
    yaml_exists = check_yaml_path(extractor, entity_type)

    if not yaml_exists:
        return True

    rule = get_rule("x_items_less_than", extractor, entity_type)
    time_delta = parse_time_frame(rule["time_frame"])
    current_time = datetime.now(UTC)
    time_frame = current_time - time_delta

    events = context.instance.get_event_records(
        EventRecordsFilter(
            asset_key=asset_key,
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
        )
    )
    if events is None:
        return True

    actual_count = events[0].asset_materialization.metadata["num_items"].value
    historical_events = get_historical_events(events)
    historic_count = get_historic_count(historical_events, time_frame)

    return actual_count >= (historic_count if historic_count > 0 else actual_count) - (
        rule["value"] or 0
    )

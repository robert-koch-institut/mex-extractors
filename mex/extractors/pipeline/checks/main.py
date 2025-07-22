from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
    AssetCheckExecutionContext,
    DagsterEventType,
    EventLogRecord,
    EventRecordsFilter,
)
from mex.extractors.pipeline.checks.models.check import AssetCheck
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


def load_asset_check_from_settings(extractor: str, entity_type: str) -> AssetCheck:
    """Load AssetCheck model from YAML for a given extractor and entity type."""
    settings = Settings.get()
    path = settings.all_checks_path / extractor / f"{entity_type}.yaml"
    return AssetCheck.model_validate(load_yaml(path))

def get_rule(rule: str,extractor: str, entity_type: str)->dict[str, Any]:
    """Load rule details from YAML file for given rule type."""
    check_model = load_asset_check_from_settings(extractor=extractor, entity_type=entity_type)
    return next(r for r in check_model.rules if r.fail_if == rule).model_dump()

def parse_time_frame(time_frame: str) -> timedelta:
    """Parse time frame string like '7d', '3m', '1y' into timedelta."""
    num = int(time_frame[:-1])
    unit = time_frame[-1]
    if unit == "m":
        return timedelta(days=30 * num)
    if unit == "y":
        return timedelta(days=365 * num)
    return timedelta(days=num)

def get_historical_events(events:list[EventLogRecord])-> dict[datetime, int]:
    """Load all past events and refactor it to a dict."""
    historical_events = [
        e for e in events if "num_items" in e.asset_materialization.metadata
    ]
    return {
        datetime.fromtimestamp(e.timestamp, tz=UTC): int(
            e.asset_materialization.metadata["num_items"].value
        )
        for e in historical_events
    }

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

def check_x_items_more_passed(context:AssetCheckExecutionContext, asset_key:str, extractor: str, entity_type: str, asset_data: int) -> Any:
    """Checks rule threshold and returns a bool."""
    rule = get_rule("x_items_more_than",extractor, entity_type)
    time_delta = parse_time_frame(rule["time_frame"])
    current_time = datetime.now(UTC)
    time_frame = current_time - time_delta

    current_time = datetime.now(UTC)
    time_frame = current_time - time_delta

    events = context.instance.get_event_records(
        EventRecordsFilter(asset_key=asset_key, event_type=DagsterEventType.ASSET_MATERIALIZATION)
    )
    #latest_event = context.instance.get_latest_materialization_event(asset_key)
    latest_count = asset_data

    historical_events = get_historical_events(events)
    historic_count = get_historic_count(historical_events, time_frame)
    return latest_count <= (historic_count if historic_count> 0 else latest_count) + (rule["value"] or 0)

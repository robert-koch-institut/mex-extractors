from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    build_asset_check_context,
)
from pytest import MonkeyPatch

from mex.extractors.pipeline.checks.main import (
    check_x_items_more_passed,
    check_yaml_path,
    get_historic_count,
    get_historical_events,
    get_rule,
    load_asset_check_from_settings,
    parse_time_frame,
)
from mex.extractors.pipeline.checks.models.check import AssetCheck, AssetCheckRule


@pytest.mark.parametrize(
    ("input_str", "expected"),
    [
        ("7d", timedelta(days=7)),
        ("3m", timedelta(days=90)),
        ("1y", timedelta(days=365)),
    ],
)
def test_parse_time_frame(input_str: str, expected: timedelta) -> None:
    assert parse_time_frame(input_str) == expected


def test_check_yaml_path(monkeypatch: MonkeyPatch) -> None:
    yaml_path = Path(__file__).parent.parent.parent / "assets" / "raw-data" / "pipeline"

    class MockSettings:
        all_checks_path = yaml_path

        @classmethod
        def get(cls) -> "MockSettings":
            return cls()

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.Settings", MockSettings)
    assert check_yaml_path("blueant", "activity") is True


def test_get_rule(monkeypatch: MonkeyPatch) -> None:
    rule_dict = {
        "fail_if": "x_items_more_than",
        "value": 10,
        "time_frame": "7d",
        "target_type": None,
    }
    check_model = AssetCheck(rules=[AssetCheckRule(**rule_dict)])

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.load_asset_check_from_settings",
        lambda *args, **kwargs: check_model,
    )

    rule = get_rule("x_items_more_than", "dummy", "dummy")
    assert rule["value"] == 10
    assert rule["time_frame"] == "7d"


def test_load_asset_check_from_settings(monkeypatch: MonkeyPatch) -> None:
    yaml_path = Path(__file__).parent.parent.parent / "assets" / "raw-data" / "pipeline"

    class MockSettings:
        all_checks_path = yaml_path

        @classmethod
        def get(cls) -> "MockSettings":
            return cls()

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.Settings", MockSettings)

    model = load_asset_check_from_settings("blueant", "activity")
    assert model.model_dump() == {
        "rules": [
            {
                "fail_if": "x_items_more_than",
                "value": 10,
                "time_frame": "10d",
                "target_type": None,
            },
            {
                "fail_if": "x_items_less_than",
                "value": 5,
                "time_frame": "2m",
                "target_type": None,
            },
        ]
    }


class DummyEventLogRecord:
    def __init__(self, timestamp: float, metadata: dict[str, Any]) -> None:
        self.timestamp = timestamp
        self.asset_materialization = SimpleNamespace(metadata=metadata)


@pytest.mark.parametrize(
    ("mock_events", "expected_values"),
    [
        (
            [
                DummyEventLogRecord(
                    timestamp=datetime(2025, 7, 29, 12, 0, tzinfo=UTC).timestamp(),
                    metadata={"num_items": SimpleNamespace(value=132)},
                ),
                DummyEventLogRecord(
                    timestamp=datetime(2025, 7, 1, 12, 0, tzinfo=UTC).timestamp(),
                    metadata={"num_items": SimpleNamespace(value=120)},
                ),
                DummyEventLogRecord(
                    timestamp=datetime(2025, 5, 1, 12, 0, tzinfo=UTC).timestamp(),
                    metadata={"num_items": SimpleNamespace(value=108)},
                ),
            ],
            [132, 120, 108],
        )
    ],
)
def test_get_historical_events(
    mock_events: list[Any], expected_values: list[int]
) -> None:
    result = get_historical_events(mock_events)
    assert sorted(result.values(), reverse=True) == expected_values


@pytest.mark.parametrize(
    ("historic_events", "time_frame", "expected_count"),
    [
        (
            {
                datetime(2025, 7, 1, 12, 0, tzinfo=UTC): 100,
                datetime(2025, 7, 20, 12, 0, tzinfo=UTC): 150,
                datetime(2025, 8, 1, 12, 0, tzinfo=UTC): 200,
            },
            datetime(2025, 7, 20, 13, 0, tzinfo=UTC),
            150,
        ),
        (
            {datetime(2025, 8, 2, 12, 0, tzinfo=UTC): 300},
            datetime(2025, 8, 1, 12, 0, tzinfo=UTC),
            300,
        ),
        (
            {},
            datetime(2025, 8, 1, 12, 0, tzinfo=UTC),
            0,
        ),
        (
            {datetime(2025, 8, 1, 12, 0, tzinfo=UTC): 777},
            datetime(2025, 8, 1, 12, 0, tzinfo=UTC),
            777,
        ),
        (
            {
                datetime(2025, 7, 1, 12, 0, tzinfo=UTC): 100,
                datetime(2025, 7, 15, 12, 0, tzinfo=UTC): 200,
                datetime(2025, 8, 1, 12, 0, tzinfo=UTC): 300,
            },
            datetime(2025, 7, 20, 12, 0, tzinfo=UTC),
            200,
        ),
    ],
    ids=[
        "closest_older_exists",
        "only_future_exists",
        "no_timestamps",
        "exact_match",
        "multiple_older_entries",
    ],
)
def test_get_historic_count(
    historic_events: dict[datetime, int], time_frame: datetime, expected_count: int
) -> None:
    result = get_historic_count(historic_events, time_frame)
    assert result == expected_count


@pytest.mark.parametrize(
    (
        "current_count",
        "historical_events",
        "rule_threshold",
        "time_frame_str",
        "passed",
    ),
    [
        (
            12,
            {
                datetime(2025, 7, 22, 12, 0, tzinfo=UTC): 10,
                datetime(2025, 7, 24, 12, 0, tzinfo=UTC): 8,
            },
            5,
            "7d",
            True,  # 12 <= 10 + 5
        ),
        (
            20,
            {
                datetime(2025, 7, 22, 12, 0, tzinfo=UTC): 10,
            },
            5,
            "7d",
            False,  # 20 > 10 + 5
        ),
        (
            15,
            {
                datetime(2025, 7, 10, 12, 0, tzinfo=UTC): 10,
                datetime(2025, 7, 25, 12, 0, tzinfo=UTC): 15,
            },
            5,
            "20d",
            True,  # 15 <= 15 + 5
        ),
        (
            15,
            {
                datetime(2025, 6, 15, 12, 0, tzinfo=UTC): 100,
            },
            0,
            "7d",
            True,  # too old, fallback to current_count + 0
        ),
        (
            30,
            {},
            5,
            "30d",
            True,  # no history => fallback to current_count + 0
        ),
    ],
    ids=[
        "passes_within_7d",
        "fails_exceeds_threshold_7d",
        "passes_within_20d_multiple_events",
        "passes_no_matching_events_due_to_time_frame",
        "passes_no_historical_events",
    ],
)
def test_check_x_items_more_passed(  # noqa: PLR0913
    monkeypatch: MonkeyPatch,
    current_count: int,
    historical_events: dict[datetime, int],
    rule_threshold: int,
    time_frame_str: str,
    *,
    passed: bool,
) -> None:
    mocked_now = datetime(2025, 8, 1, 12, 0, tzinfo=UTC)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> "FixedDatetime":
            dt = mocked_now.astimezone(tz) if tz else mocked_now.replace(tzinfo=None)
            return cls(
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second,
                dt.microsecond,
                dt.tzinfo,
            )

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.datetime", FixedDatetime)
    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.get_rule",
        lambda *args, **kwargs: {"value": rule_threshold, "time_frame": time_frame_str},
    )

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.check_yaml_path",
        lambda *args, **kwargs: True,
    )

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.get_historical_events",
        lambda *args, **kwargs: historical_events,
    )

    instance = DagsterInstance.ephemeral()
    context = build_asset_check_context(instance=instance)
    asset_key = AssetKey(["some_asset"])
    result = check_x_items_more_passed(context, asset_key, "ext", "type", current_count)
    assert result is passed

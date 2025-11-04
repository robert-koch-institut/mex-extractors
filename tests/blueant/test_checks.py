from datetime import UTC, datetime, tzinfo
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
    fail_if_item_count_is_x_items_less_than,
)


@pytest.mark.parametrize(
    (
        "yaml_exists",
        "rule_threshold",
        "time_frame_str",
        "events",
        "current_count",
        "expected_passed",
    ),
    [
        pytest.param(
            True,
            5,
            "7d",
            [
                {
                    "timestamp": datetime(2025, 7, 26, 12, 0, tzinfo=UTC).timestamp(),
                    "metadata": {"num_items": 10},
                }
            ],
            14,
            True,
            id="passes_within_threshold",
        ),
        pytest.param(
            True,
            5,
            "7d",
            [
                {
                    "timestamp": datetime(2025, 7, 26, 12, 0, tzinfo=UTC).timestamp(),
                    "metadata": {"num_items": 10},
                }
            ],
            20,
            False,
            id="fails_exceeds_threshold",
        ),
        pytest.param(
            True,
            0,
            "1d",
            [],
            99,
            True,
            id="no_history_fallback",
        ),
        pytest.param(
            False,
            None,
            None,
            [],
            999,
            True,
            id="yaml_not_found_should_pass",
        ),
    ],
)
def test_check_x_items_more_passed_parametrized(  # noqa: PLR0913
    monkeypatch: MonkeyPatch,
    rule_threshold: int,
    time_frame_str: str,
    events: list[dict[str, Any]],
    current_count: int,
    *,
    expected_passed: bool,
    yaml_exists: bool,
) -> None:
    mocked_now = datetime(2025, 8, 1, 12, 0, tzinfo=UTC)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> "FixedDatetime":
            return cls.fromtimestamp(mocked_now.astimezone(tz).timestamp(), tz=tz)

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.datetime", FixedDatetime)

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.check_yaml_path",
        lambda extractor, entity_type: yaml_exists,
    )

    if yaml_exists:
        monkeypatch.setattr(
            "mex.extractors.pipeline.checks.main.get_rule",
            lambda *_, **__: {"value": rule_threshold, "time_frame": time_frame_str},
        )

    class DummyMaterialization:
        def __init__(self, metadata: dict[str, SimpleNamespace]) -> None:
            self.metadata = {"num_items": SimpleNamespace(value=metadata["num_items"])}

    class DummyEvent:
        def __init__(
            self, timestamp: datetime, metadata: dict[str, SimpleNamespace]
        ) -> None:
            self.timestamp = timestamp
            self.asset_materialization = DummyMaterialization(metadata)

    mocked_event_records = [DummyEvent(e["timestamp"], e["metadata"]) for e in events]

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.get_historical_events",
        lambda event_list: {
            datetime.fromtimestamp(e.timestamp, tz=UTC): int(
                e.asset_materialization.metadata["num_items"].value
            )
            for e in event_list
        },
    )

    context = build_asset_check_context(instance=DagsterInstance.ephemeral())
    context.instance.get_event_records = lambda *_, **__: mocked_event_records

    result = check_x_items_more_passed(
        context=context,
        asset_key=AssetKey(["test_asset"]),
        extractor="some_ext",
        entity_type="some_type",
        asset_data=current_count,
    )

    assert result is expected_passed


@pytest.mark.parametrize(
    (
        "yaml_exists",
        "rule_threshold",
        "time_frame_str",
        "events",
        "current_count",
        "expected_passed",
    ),
    [
        pytest.param(
            True,
            5,
            "7d",
            [
                {
                    "timestamp": datetime(2025, 7, 26, 12, 0, tzinfo=UTC).timestamp(),
                    "metadata": {"num_items": 10},
                }
            ],
            14,
            True,
            id="passes_within_threshold",
        ),
        pytest.param(
            True,
            5,
            "7d",
            [
                {
                    "timestamp": datetime(2025, 7, 26, 12, 0, tzinfo=UTC).timestamp(),
                    "metadata": {"num_items": 10},
                }
            ],
            4,
            False,
            id="fails_exceeds_threshold",
        ),
        pytest.param(
            True,
            0,
            "1d",
            [],
            99,
            True,
            id="no_history_fallback",
        ),
        pytest.param(
            False,
            None,
            None,
            [],
            999,
            True,
            id="yaml_not_found_should_pass",
        ),
    ],
)
def test_check_x_items_less_passed_parametrized(  # noqa: PLR0913
    monkeypatch: MonkeyPatch,
    rule_threshold: int,
    time_frame_str: str,
    events: list[dict[str, Any]],
    current_count: int,
    *,
    expected_passed: bool,
    yaml_exists: bool,
) -> None:
    mocked_now = datetime(2025, 8, 1, 12, 0, tzinfo=UTC)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> "FixedDatetime":
            return cls.fromtimestamp(mocked_now.astimezone(tz).timestamp(), tz=tz)

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.datetime", FixedDatetime)

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.check_yaml_path",
        lambda extractor, entity_type: yaml_exists,
    )

    if yaml_exists:
        monkeypatch.setattr(
            "mex.extractors.pipeline.checks.main.get_rule",
            lambda *_, **__: {"value": rule_threshold, "time_frame": time_frame_str},
        )

    class DummyMaterialization:
        def __init__(self, metadata: dict[str, SimpleNamespace]) -> None:
            self.metadata = {"num_items": SimpleNamespace(value=metadata["num_items"])}

    class DummyEvent:
        def __init__(
            self, timestamp: datetime, metadata: dict[str, SimpleNamespace]
        ) -> None:
            self.timestamp = timestamp
            self.asset_materialization = DummyMaterialization(metadata)

    mocked_event_records = [DummyEvent(e["timestamp"], e["metadata"]) for e in events]

    monkeypatch.setattr(
        "mex.extractors.pipeline.checks.main.get_historical_events",
        lambda event_list: {
            datetime.fromtimestamp(e.timestamp, tz=UTC): int(
                e.asset_materialization.metadata["num_items"].value
            )
            for e in event_list
        },
    )

    context = build_asset_check_context(instance=DagsterInstance.ephemeral())
    context.instance.get_event_records = lambda *_, **__: mocked_event_records
    result = fail_if_item_count_is_x_items_less_than(
        context=context,
        asset_key=AssetKey(["test_asset"]),
        extractor="some_ext",
        entity_type="some_type",
        asset_data=current_count,
    )

    assert result is expected_passed

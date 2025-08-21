from datetime import UTC, datetime, tzinfo
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from dagster import (
    AssetCheckResult,
    AssetKey,
    DagsterInstance,
    build_asset_check_context,
)
from pytest import MonkeyPatch

from mex.extractors.blueant.checks import check_yaml_rules_exist
from mex.extractors.pipeline.checks.main import check_x_items_more_passed


def test_check_yaml_rules_exist_with_real_yaml(monkeypatch: MonkeyPatch) -> None:
    yaml_path = Path(__file__).parent.parent.parent / "assets" / "raw-data" / "pipeline"

    class MockSettings:
        all_checks_path = yaml_path

        @classmethod
        def get(cls) -> "MockSettings":
            return cls()

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.Settings", MockSettings)

    context = build_asset_check_context()
    result = check_yaml_rules_exist(context)
    assert isinstance(result, AssetCheckResult)
    assert result.passed


@pytest.mark.parametrize(
    ("rule_threshold", "expected_passed"),
    [
        (None, False),
        (0, False),
        (-1, False),
        (5, True),
    ],
    ids=["none", "0", "negative_value", "valid_value"],
)
def test_check_yaml_rules_exist_valid_threshold(
    monkeypatch: MonkeyPatch, rule_threshold: int, *, expected_passed: bool
) -> None:
    yaml_path = Path(__file__).parent.parent.parent / "assets" / "raw-data" / "pipeline"

    class MockSettings:
        all_checks_path = yaml_path

        @classmethod
        def get(cls) -> "MockSettings":
            return cls()

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.Settings", MockSettings)

    monkeypatch.setattr(
        "mex.extractors.blueant.checks.check_yaml_path",
        lambda extractor, entity_type: True,
    )

    monkeypatch.setattr(
        "mex.extractors.blueant.checks.get_rule",
        lambda rule, extractor, entity_type: {"value": rule_threshold},
    )

    context = build_asset_check_context()
    result = check_yaml_rules_exist(context)
    assert isinstance(result, AssetCheckResult)
    assert result.passed == expected_passed


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
        (
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
        ),
        (
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
        ),
        (
            True,
            0,
            "1d",
            [],
            99,
            True,
        ),
        (
            False,
            None,
            None,
            [],
            999,
            True,
        ),
    ],
    ids=[
        "passes_within_threshold",
        "fails_exceeds_threshold",
        "no_history_fallback",
        "yaml_not_found_should_pass",
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

    dummy_event_records = [DummyEvent(e["timestamp"], e["metadata"]) for e in events]

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
    context.instance.get_event_records = lambda *_, **__: dummy_event_records

    result = check_x_items_more_passed(
        context=context,
        asset_key=AssetKey(["test_asset"]),
        extractor="some_ext",
        entity_type="some_type",
        asset_data=current_count,
    )

    assert result is expected_passed

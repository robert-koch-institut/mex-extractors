from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetKey,
    DagsterInstance,
    RunRequest,
    SkipReason,
    asset,
    build_sensor_context,
)
from dagster._core.test_utils import instance_for_test
from pytest import MonkeyPatch

from mex.extractors.pipeline.base import load_job_definitions, monitor_jobs_sensor
from mex.extractors.pipeline.checks.models.check import AssetCheck, AssetCheckRule


def test_monitor_skip_if_jobs_are_running() -> None:
    publisher_run = MagicMock(end_time=1000)  # unix time notation
    extractor_run = MagicMock(end_time=1100)  # newer than publisher

    with instance_for_test() as test_instance:
        # First get_run_records returns a running job
        running_job = MagicMock()
        test_instance.get_run_records = MagicMock(  # type: ignore[method-assign]
            side_effect=[
                [running_job],  # Other jobs are still running
                [publisher_run],  # Publisher run
                [extractor_run],  # Extractor run
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(
                instance=test_instance,
            )

            sensor = monitor_jobs_sensor
            result = sensor(context)

            assert isinstance(result, SkipReason)
            assert (
                result.skip_message
                == "No publishing because jobs are running at the moment."
            )


def test_monitor_skip_if_no_complete_run() -> None:
    publisher_run = MagicMock(end_time=1000)
    extractor_run = MagicMock(end_time=900)  # older than publisher

    with instance_for_test() as test_instance:
        test_instance.get_run_records = MagicMock(  # type: ignore[method-assign]
            side_effect=[
                [],  # No running jobs
                [publisher_run],  # Publisher run
                [extractor_run],  # Extractor run
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(instance=test_instance)

            result = monitor_jobs_sensor(context)

            assert isinstance(result, SkipReason)
            assert (
                result.skip_message
                == "No complete unpublished run for any extractor job yet."
            )


def test_monitor_triggers_if_new_jobs_finished() -> None:
    publisher_run = MagicMock(end_time=1000)
    extractor_run = MagicMock(end_time=1100)  # newer than publisher

    with instance_for_test() as test_instance:
        test_instance.get_run_records = MagicMock(  # type: ignore[method-assign]
            side_effect=[
                [],  # No running jobs
                [publisher_run],  # Publisher run
                [extractor_run],  # Extractor run
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(instance=test_instance)

            result = monitor_jobs_sensor(context)

            assert isinstance(result, RunRequest)
            assert result.run_key == "1100"


@pytest.mark.parametrize(
    (
        "group_name",
        "metadata",
        "return_value",
        "rules",
        "expected_check_created",
    ),
    [
        (
            "blueant",
            {"entity_type": "activity"},
            1,
            [{"fail_if": "x_items_more_than", "value": 5, "time_frame": "7d"}],
            1,
        ),
        (
            "blueant",
            {},
            2,
            [{"fail_if": "x_items_more_than", "value": 5, "time_frame": "7d"}],
            0,
        ),
        (
            "international_projects",
            {"entity_type": "activity"},
            3,
            [
                {"fail_if": "x_items_more_than", "value": 20, "time_frame": "7d"},
                {"fail_if": "x_items_less_than", "value": 5, "time_frame": "7d"},
            ],
            2,
        ),
    ],
    ids=["one_check_created", "no_check_created", "two_checks_created"],
)
def test_asset_checks_created(  # noqa: PLR0913
    monkeypatch: MonkeyPatch,
    group_name: str,
    metadata: dict[str, str],
    return_value: int,
    rules: list[dict[str, str]],
    expected_check_created: int,
) -> None:
    @asset(key=AssetKey(["test_asset"]), group_name=group_name, metadata=metadata)
    def test_asset() -> int:
        return return_value

    monkeypatch.setattr(
        "mex.extractors.pipeline.base.load_assets_from_package_module",
        lambda module: [test_asset],
    )

    load_calls = []

    def load_mocked_asset_check(group_name: str, entity_name: str) -> AssetCheck:  # noqa: ARG001
        load_calls.append(entity_name)
        return AssetCheck(rules=[AssetCheckRule(**rule) for rule in rules])

    monkeypatch.setattr(
        "mex.extractors.pipeline.base.load_asset_check_from_settings",
        load_mocked_asset_check,
    )

    defs = load_job_definitions()

    assert len(list(defs.asset_checks or [])) == expected_check_created

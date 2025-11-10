from collections import defaultdict
from collections.abc import Generator
from typing import Any, Optional
from unittest.mock import MagicMock, patch

from dagster import (
    AssetKey,
    AssetsDefinition,
    DagsterInstance,
    RunRequest,
    SkipReason,
    asset,
    build_sensor_context,
)
from dagster._core.test_utils import instance_for_test
import pytest

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


# @pytest.mark.parametrize(
#     (
#         "asset_metadata",
#         "rules",
#         "expected_check_created",
#     ),
#     [
#         # Single asset with one x_items_more_than rule
#         (
#             {"entity_type": "user"},
#             [{"fail_if": "x_items_more_than", "value": 5, "time_frame": "7d"}],
#             True,
#         ),
#         # Single asset with one x_items_less_than rule
#         (
#             {"entity_type": "order"},
#             [{"fail_if": "x_items_less_than", "value": 10, "time_frame": "7d"}],
#             True,
#         ),
#         # Single asset with no entity_type → no checks
#         (
#             {},
#             [{"fail_if": "x_items_more_than", "value": 5, "time_frame": "7d"}],
#             False,
#         ),
#         # Single asset with multiple rules
#         (
#             {"entity_type": "payment"},
#             [
#                 {"fail_if": "x_items_more_than", "value": 5, "time_frame": "7d"},
#                 {"fail_if": "x_items_less_than", "value": 2, "time_frame": "7d"},
#             ],
#             True,
#         ),
#     ],
# )
def test_asset_checks_created(
    monkeypatch: pytest.MonkeyPatch,
    # asset_metadata: dict[str, str],
    # rules: dict,
    # *,
    # expected_check_created: bool,
) -> None:
    @asset(group_name="group_1", metadata={"entity_type": "activity"})
    def asset_with_entity()-> int:
            return 1

        # Asset without entity_type → should not create checks
    @asset(group_name="group_2", metadata={})
    def asset_without_entity()-> int:
        return 2

    mocked_assets = [asset_with_entity, asset_without_entity]
    monkeypatch.setattr(
        "mex.extractors.pipeline.base.load_assets_from_package_module",
        lambda module: mocked_assets,
    )
    load_calls = []
    def load_asset_check_mock(group_name, entity_name) -> AssetCheck:
        load_calls.append(entity_name)
        return AssetCheck(
            rules=[AssetCheckRule(fail_if="x_items_more_than", value=5, time_frame="7d")]
        )
    # check_model = AssetCheck(rules=[AssetCheckRule(**rules)])

    # monkeypatch.setattr(
    #     "mex.extractors.pipeline.base.load_asset_check_from_settings",
    #     lambda *args, **kwargs: check_model,
    # )

    defs = load_job_definitions()

    checks_created = len(defs.asset_checks) > 0
    assert checks_created

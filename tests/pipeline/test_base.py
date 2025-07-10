from unittest.mock import MagicMock, patch

from dagster import (
    DagsterInstance,
    RunRequest,
    SkipReason,
    build_sensor_context,
)
from dagster._core.test_utils import instance_for_test

from mex.extractors.pipeline.base import (
    monitor_jobs_sensor,
)


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

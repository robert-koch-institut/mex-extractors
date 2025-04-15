from unittest.mock import MagicMock, patch

from dagster import (
    DagsterInstance,
    DagsterRunStatus,
    RunRequest,
    SkipReason,
    build_sensor_context,
)
from dagster._core.test_utils import instance_for_test

from mex.extractors.pipeline.base import (
    create_monitor_jobs_sensor,
)


def test_monitor_skip_if_jobs_are_running():
    with instance_for_test() as test_instance:
        # First get_run_records returns a running job
        running_job = MagicMock()
        test_instance.get_run_records = MagicMock(
            side_effect=[
                [running_job],  # Other jobs are still running
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(
                cursor="1700000000",
                instance=test_instance,
            )

            sensor = create_monitor_jobs_sensor(["example_job"])
            result = sensor(context)

            assert (
                result.skip_message
                == "No publishing because other jobs are running at the moment."
            )


def test_monitor_skip_if_no_complete_runs():
    with instance_for_test() as test_instance:
        test_instance.get_run_records = MagicMock(
            side_effect=[
                [],  # No running jobs
                [],  # No finished extractor runs
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(
                cursor="1700000000",
                instance=test_instance,
            )

            sensor = create_monitor_jobs_sensor(["example_job"])
            result = sensor(context)

            assert isinstance(result, SkipReason)
            assert (
                result.skip_message
                == "No complete run for job group 'example_job' yet."
            )


def test_monitor_triggers_if_all_jobs_finished():
    with instance_for_test() as test_instance:
        mock_run = MagicMock()  # mock example extractor job
        mock_run.status = DagsterRunStatus.SUCCESS
        mock_run.end_time = 1700000007

        test_instance.get_run_records = MagicMock(
            side_effect=[
                [],  # No jobs running
                [mock_run],  # Completed extractor job since last publisher run
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(
                cursor="1700000000",  # last publisher run
                instance=test_instance,
            )

            sensor = create_monitor_jobs_sensor(["example_job"])
            result = sensor(context)

            assert isinstance(result, RunRequest)
            assert result.run_key == "1700000007"


def test_monitor_skips_for_old_runs():
    with instance_for_test() as test_instance:
        mock_run = MagicMock()  # mock example extractor job
        mock_run.status = DagsterRunStatus.SUCCESS
        mock_run.end_time = 0

        test_instance.get_run_records = MagicMock(
            side_effect=[
                [],  # No jobs running
                [mock_run],  # Completed extractor job from 1970
            ]
        )

        with patch.object(DagsterInstance, "get", return_value=test_instance):
            context = build_sensor_context(
                cursor="1700000000",  # last publisher run
                instance=test_instance,
            )

            sensor = create_monitor_jobs_sensor(["example_job"])
            result = sensor(context)

            assert isinstance(result, SkipReason)
            assert (
                result.skip_message
                == "No complete run for job group 'example_job' yet."
            )

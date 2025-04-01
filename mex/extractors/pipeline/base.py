from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from dagster import (
    AssetsDefinition,
    AssetSelection,
    DagsterInstance,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    FilesystemIOManager,
    MetadataValue,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    load_assets_from_package_module,
    sensor,
)

from mex.extractors.settings import Settings

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence

    from dagster._core.execution.execution_result import ExecutionResult


def run_job_in_process(group_name: str = "default") -> "ExecutionResult":
    """Run the dagster job with the given group name locally in-process."""
    from mex.extractors import defs  # avoid circular imports

    job = defs.get_job_def(group_name)
    return job.execute_in_process()


def create_monitor_jobs_sensor(group_names: list[str]) -> SensorDefinition:
    """Wrapper to be apple to pass group_names variable."""

    @sensor(
        job_name="publisher",
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=1800,  # 30 min
    )
    def monitor_jobs_sensor(
        context: SensorEvaluationContext,  # noqa: ARG001
    ) -> RunRequest | SkipReason:
        """Sensor to monitor the completion of all extractors and trigger publisher."""
        instance = DagsterInstance.get()
        today = datetime.now(tz=UTC).strftime("%Y-%m-%d")  # to use as run_key

        for group in group_names:
            runs = instance.get_runs(
                filters=RunsFilter(
                    job_name=group,
                    statuses=[DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE],
                )
            )

            if not any(
                run.status in {DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE}
                for run in runs
            ):
                return SkipReason(f"No completed runs for job group '{group}' yet.")

        # All jobs have completed at least once (success or failure),
        # trigger the final job
        return RunRequest(run_key=today, run_config={})

    return monitor_jobs_sensor


def load_job_definitions() -> Definitions:
    """Scan the mex package for assets, define jobs and io and return definitions."""
    import mex  # avoid circular imports

    settings = Settings.get()

    resources = {"io_manager": FilesystemIOManager()}
    assets = cast("Sequence[AssetsDefinition]", load_assets_from_package_module(mex))
    group_names = {
        group for asset in assets for group in asset.group_names_by_key.values()
    }
    settings_md = MetadataValue.md(f"```json\n{settings.model_dump_json(indent=4)}```")
    jobs = [
        define_asset_job(
            group_name,
            AssetSelection.groups(group_name).upstream(),
            metadata={"settings": settings_md},
        )
        for group_name in group_names
        if group_name not in ["default", *settings.skip_extractors]
    ]

    schedules = [
        ScheduleDefinition(
            job=job,
            cron_schedule=settings.schedule,
            default_status=DefaultScheduleStatus.RUNNING,
        )
        for job in jobs
    ]

    # Define the extra job
    publisher_job = define_asset_job(
        "publisher",
        AssetSelection.groups("publisher").upstream(),
    )
    jobs.append(publisher_job)

    sensor = create_monitor_jobs_sensor(list(group_names))

    jobs.append(
        define_asset_job(
            "all_extractors",
            AssetSelection.groups(
                *(group_name for group_name in group_names)
            ).upstream(),
        )
    )

    return Definitions(
        assets=assets,
        jobs=jobs,
        resources=resources,
        schedules=schedules,
        sensors=[sensor],
    )

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


def create_monitor_jobs_sensor(extractor_group_names: list[str]) -> SensorDefinition:
    """Wrapper to be able to pass group_names variable."""

    @sensor(
        job_name="publisher",
        default_status=DefaultSensorStatus.RUNNING,
        minimum_interval_seconds=60*60,
    )
    def monitor_jobs_sensor(
        context: SensorEvaluationContext,
    ) -> RunRequest | SkipReason:
        """Sensor to monitor the completion of all extractors and trigger publisher."""
        instance = DagsterInstance.get()
        last_run_time_publisher = context.cursor or "1970-01-01T00:00:00+00:00"
        latest_start_time_extractors = last_run_time_publisher
        completed = True

        jobs_are_running = instance.get_runs(
            filters=RunsFilter(
                statuses=[
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.STARTED,
                    DagsterRunStatus.CANCELING,
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                ],
            )
        )
        if jobs_are_running:
            return SkipReason(
                "No publishing because other jobs are running at the moment."
            )

        for group in extractor_group_names:
            runs = instance.get_runs(
                filters=RunsFilter(
                    job_name=group,
                    statuses=[
                        DagsterRunStatus.SUCCESS,
                        DagsterRunStatus.FAILURE,
                        DagsterRunStatus.CANCELED,
                    ],
                )
            )

            freshest_unpublished_run = max(
                (
                    run
                    for run in runs
                    if ".dagster/scheduled_execution_time" in run.tags
                    and run.tags[".dagster/scheduled_execution_time"]
                    > last_run_time_publisher
                ),
                key=lambda run: run.tags[".dagster/scheduled_execution_time"],
                default=None,
            )

            # Update completed flag based on whether a recent run was found
            if recent_run is None or recent_run.status not in [
                DagsterRunStatus.SUCCESS,
                DagsterRunStatus.FAILURE,
                DagsterRunStatus.CANCELED,
            ]:
                completed = False
                return SkipReason(f"No complete run for job group '{group}' yet.")

            # Update the latest extractor start time if the current run is newer
            if (
                recent_run
                and recent_run.tags[".dagster/scheduled_execution_time"]
                > latest_start_time_extractors
            ):
                latest_start_time_extractors = recent_run.tags[
                    ".dagster/scheduled_execution_time"
                ]

        if completed:
            # Update the cursor to the latest extractor start time
            context.update_cursor(latest_start_time_extractors)
            return RunRequest(
                run_key=latest_start_time_extractors,
                run_config={},
            )

        return SkipReason("No publisher run for other reasons.")

    return monitor_jobs_sensor


def load_job_definitions() -> Definitions:
    """Scan the mex package for assets, define jobs and io and return definitions."""
    import mex  # avoid circular imports

    settings = Settings.get()

    resources = {"io_manager": FilesystemIOManager()}
    assets = cast("Sequence[AssetsDefinition]", load_assets_from_package_module(mex))
    extractor_group_names = {
        group
        for asset in assets
        for group in asset.group_names_by_key.values()
        if group not in ["default", "publisher", *settings.skip_extractors]
    }
    settings_md = MetadataValue.md(f"```json\n{settings.model_dump_json(indent=4)}```")
    jobs = [
        define_asset_job(
            group_name,
            AssetSelection.groups(group_name).upstream(),
            metadata={"settings": settings_md},
        )
        for group_name in extractor_group_names
    ]

    schedules = [
        ScheduleDefinition(
            job=job,
            cron_schedule=settings.schedule,
            default_status=DefaultScheduleStatus.RUNNING,
        )
        for job in jobs
    ]

    jobs.append(
        define_asset_job(
            "all_extractors",
            AssetSelection.groups(
                *extractor_group_names
            ).upstream(),
            metadata={"settings": settings_md},
        )
    )

    # Define the extra publisher job
    publisher_job = define_asset_job(
        "publisher",
        AssetSelection.groups("publisher").upstream(),
        metadata={"settings": settings_md},
    )
    jobs.append(publisher_job)

    sensor = create_monitor_jobs_sensor(list(extractor_group_names))

    return Definitions(
        assets=assets,
        jobs=jobs,
        resources=resources,
        schedules=schedules,
        sensors=[sensor],
    )

from typing import cast

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
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    load_asset_checks_from_package_module,
    load_assets_from_package_module,
    sensor,
)

from mex.extractors.settings import Settings


def run_job_in_process(group_name: str = "default") -> bool:
    """Run the dagster job with the given group name locally in-process."""
    from mex.extractors import defs  # avoid circular imports

    job = defs.get_job_def(group_name)
    result = job.execute_in_process()
    return result.success


@sensor(
    job_name="publisher",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60 * 5,
)
def monitor_jobs_sensor(
    context: SensorEvaluationContext,  # noqa: ARG001
) -> RunRequest | SkipReason:
    """Sensor to monitor the completion of all extractors and trigger publisher."""
    instance = DagsterInstance.get()

    if instance.get_run_records(
        filters=RunsFilter(
            statuses=[
                DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED,
                DagsterRunStatus.CANCELING,
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.NOT_STARTED,
            ],
        )
    ):
        return SkipReason("No publishing because jobs are running at the moment.")

    publisher_runs = instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"job_category": "publisher"},
        )
    )

    newest_publisher_run_ts = max(
        (
            run.end_time  # given in unix time notation
            for run in publisher_runs
            if run.end_time
        ),
        default=0.0,  # unix time notation for 1970
    )

    extractor_runs = instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"job_category": "extractor"},
        )
    )

    newest_extractor_run_ts = max(
        (run.end_time for run in extractor_runs if run.end_time),
        default=0.0,
    )

    if newest_publisher_run_ts > newest_extractor_run_ts:
        return SkipReason("No complete unpublished run for any extractor job yet.")

    return RunRequest(
        run_key=str(newest_extractor_run_ts),
        run_config={},
    )


def load_job_definitions() -> Definitions:
    """Scan the mex package for assets, define jobs and io and return definitions."""
    import mex  # avoid circular imports

    settings = Settings.get()

    resources = {"io_manager": FilesystemIOManager()}
    assets = cast("list[AssetsDefinition]", load_assets_from_package_module(mex))
    checks = load_asset_checks_from_package_module(mex)

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
            tags={"job_category": "extractor"},
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
            AssetSelection.groups(*extractor_group_names).upstream(),
            metadata={"settings": settings_md},
            tags={"job_category": "extractor"},
        )
    )

    # Define the extra publisher job
    publisher_job = define_asset_job(
        "publisher",
        AssetSelection.groups("publisher").upstream(),
        metadata={"settings": settings_md},
        tags={"job_category": "publisher"},
    )
    jobs.append(publisher_job)

    return Definitions(
        assets=assets,
        asset_checks=checks,
        jobs=jobs,
        resources=resources,
        schedules=schedules,
        sensors=[monitor_jobs_sensor],
    )

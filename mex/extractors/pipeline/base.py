from importlib import import_module
from importlib.metadata import version
from typing import Any, cast

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
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
    asset_check,
    define_asset_job,
    load_assets_from_package_module,
    sensor,
)

from mex.extractors.pipeline.checks.main import load_asset_check_from_settings
from mex.extractors.settings import Settings


def run_job_in_process(group_name: str) -> bool:
    """Run the dagster job with the given group name locally in-process."""
    defs = load_job_definitions()
    job = defs.resolve_job_def(group_name)
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


def create_asset_check_more_than(
    asset_key: AssetKey, group_name: str, entity_name: str
) -> AssetCheckResult:
    """Creating dynamically asset check for more_than rule."""

    @asset_check(
        asset=asset_key,
        blocking=True,
    )
    def check_x_items_more_than(
        context: AssetCheckExecutionContext,
    ) -> AssetCheckResult:
        """Check that latest count is not more than threshold + historical count."""
        from mex.extractors.pipeline.checks.main import (  # noqa: PLC0415
            check_x_items_more_passed,
        )

        passed = check_x_items_more_passed(context, asset_key, group_name, entity_name)
        return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)

    return check_x_items_more_than


def create_asset_check_less_than(
    asset_key: AssetKey, group_name: str, entity_name: str
) -> AssetCheckResult:
    """Creating dynamically asset check for less_than rule."""

    @asset_check(
        asset=asset_key,
        blocking=True,
    )
    def check_x_items_less_than(
        context: AssetCheckExecutionContext,
    ) -> AssetCheckResult:
        """Check that latest count is not less than threshold."""
        from mex.extractors.pipeline.checks.main import (  # noqa: PLC0415
            fail_if_item_count_is_x_items_less_than,
        )

        passed = fail_if_item_count_is_x_items_less_than(
            context, asset_key, group_name, entity_name
        )
        return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)

    return check_x_items_less_than


def load_job_definitions() -> Definitions:
    """Scan the given module for assets, define jobs and io and return definitions."""
    settings = Settings.get()

    resources = {"io_manager": FilesystemIOManager()}
    module = import_module("mex.extractors")
    assets = load_assets_from_package_module(module)

    checks = []
    for asset in assets:
        items = list(asset.group_names_by_key.items())
        if len(items) != 1:
            msg = f"asset has more than 1 group: {items}"
            raise RuntimeError(msg)
        asset_key, group_name = items[0]
        asset_metadata_dict = dict(asset.metadata_by_key.items())
        if "entity_type" in asset_metadata_dict.get(asset_key, {}):
            entity_name = asset_metadata_dict[asset_key]["entity_type"]

            try:
                rules_for_asset = load_asset_check_from_settings(
                    group_name, entity_name
                )
            except FileNotFoundError:
                continue

            for rule in rules_for_asset.rules:
                if rule.fail_if == "x_items_more_than":
                    check_more_than = create_asset_check_more_than(
                        asset_key, group_name, entity_name
                    )
                    checks.append(check_more_than)
                if rule.fail_if == "x_items_less_than":
                    check_less_than = create_asset_check_less_than(
                        asset_key, group_name, entity_name
                    )
                    checks.append(check_less_than)

    extractor_group_names = {
        group
        for asset in assets
        for group in cast("AssetsDefinition", asset).group_names_by_key.values()
        if group
        not in [
            "default",
            "publisher",
            "consent_mailer",
            "datenkompass",
            *settings.skip_extractors,
        ]
    }
    metadata: dict[str, Any] = {
        "settings": MetadataValue.md(
            f"```json\n{settings.model_dump_json(indent=4)}```"
        ),
        "version": MetadataValue.text(version("mex-extractors")),
    }

    jobs = [
        define_asset_job(
            group_name,
            AssetSelection.groups(group_name).upstream(),
            metadata={**metadata, "group_name": group_name},
            tags={"job_category": "extractor"},
        )
        for group_name in extractor_group_names
    ]

    sensors = []

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
            metadata=metadata,
            tags={"job_category": "extractor"},
        )
    )

    # configure consent mailer
    consent_mailer_job = define_asset_job(
        "consent_mailer",
        AssetSelection.groups("consent_mailer").upstream(),
        metadata=metadata,
        tags={"job_category": "consent_mailer"},
    )
    jobs.append(consent_mailer_job)
    if settings.consent_mailer.schedule:
        schedules.append(
            ScheduleDefinition(
                job=consent_mailer_job,
                cron_schedule=settings.consent_mailer.schedule,
                default_status=DefaultScheduleStatus.RUNNING,
            )
        )

    # Define the extra datenkompass job but without schedule or trigger
    datenkompass_job = define_asset_job(
        "datenkompass",
        AssetSelection.groups("datenkompass").upstream(),
        metadata=metadata,
        tags={"job_category": "publisher"},
    )
    jobs.append(datenkompass_job)

    # Define the extra publisher job
    publisher_job = define_asset_job(
        "publisher",
        AssetSelection.groups("publisher").upstream(),
        metadata=metadata,
        tags={"job_category": "publisher"},
    )
    jobs.append(publisher_job)
    sensors.append(monitor_jobs_sensor)

    # Define dagster code location
    return Definitions(
        assets=assets,
        asset_checks=checks,
        jobs=jobs,
        resources=resources,
        schedules=schedules,
        sensors=sensors,
    )

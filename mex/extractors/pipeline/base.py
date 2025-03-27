from typing import TYPE_CHECKING, cast

from dagster import (
    AssetsDefinition,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    FilesystemIOManager,
    MetadataValue,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
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
    jobs.append(
        define_asset_job(
            "all_extractors",
            AssetSelection.groups(
                *(
                    group_name
                    for group_name in group_names
                    if group_name not in ["publisher", *settings.skip_extractors]
                )
            ).upstream(),
        )
    )
    return Definitions(
        assets=assets,
        jobs=jobs,
        resources=resources,
        schedules=schedules,
    )

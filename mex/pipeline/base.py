from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from dagster import (
    AssetsDefinition,
    AssetSelection,
    Definitions,
    FilesystemIOManager,
    define_asset_job,
    load_assets_from_package_module,
)

if TYPE_CHECKING:  # pragma: no cover
    from dagster._core.execution.execution_result import ExecutionResult


def run_job_in_process(group_name: str = "default") -> "ExecutionResult":
    """Run the dagster job with the given group name locally in-process."""
    import mex  # avoid circular imports

    job = mex.defs.get_job_def(group_name)
    return job.execute_in_process()


def load_job_definitions() -> Definitions:
    """Scan the mex package for assets, define jobs and io and return definitions."""
    import mex  # avoid circular imports

    resources = {"io_manager": FilesystemIOManager()}
    assets = cast(Sequence[AssetsDefinition], load_assets_from_package_module(mex))
    group_names = {
        group for asset in assets for group in asset.group_names_by_key.values()
    }
    jobs = [
        define_asset_job(group_name, AssetSelection.groups(group_name).upstream())
        for group_name in group_names
        if group_name != "default"
    ]
    jobs.append(
        define_asset_job(
            "all_assets",
            AssetSelection.groups(
                *[group for group in group_names if group != "synopse"]
            ).upstream(),
        )
    )
    return Definitions(assets=assets, jobs=jobs, resources=resources)

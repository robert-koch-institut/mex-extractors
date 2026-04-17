import datetime
import json
import shutil
import subprocess
from pathlib import Path

from dagster import AssetKey, DagsterInstance, RunsFilter, asset
from pytz import UTC

from mex.common.logging import logger
from mex.extractors.pipeline.base import load_job_definitions
from mex.extractors.settings import Settings


@asset(group_name="system_clean_up")
def system_fetch_old_dagster_run_ids() -> list[str]:
    """Fetch ids of Dagster runs, which are old enough to be deleted."""
    instance = DagsterInstance.get()
    settings = Settings.get()
    # calculate the date threshold
    cutoff_date = datetime.datetime.now(tz=UTC) - datetime.timedelta(
        days=settings.system.max_run_age_in_days
    )

    old_run_records = instance.get_run_records(
        filters=RunsFilter(created_before=cutoff_date),
        limit=100,  # Process in batches
        ascending=True,  # start with oldest
    )

    return [r.dagster_run.run_id for r in old_run_records]


@asset(group_name="system_clean_up")
def system_clean_up_dagster_files(
    system_fetch_old_dagster_run_ids: list[str],
) -> list[str]:
    """Clean up dagster files of old runs. Note: not all runs have files."""
    instance = DagsterInstance.get()
    storage_path = Path(instance.storage_directory())

    deleted_file_ids: list[str] = []

    for run_id in system_fetch_old_dagster_run_ids:
        run_storage_path = storage_path / run_id
        try:
            shutil.rmtree(run_storage_path)
            deleted_file_ids.append(run_id)
        except FileNotFoundError:
            logger.warning("File not found for: %s", run_id)
            continue

    logger.info(
        "Deleted %s folders of old runs from %s.",
        len(deleted_file_ids),
        storage_path,
    )
    return deleted_file_ids


@asset(group_name="system_clean_up", deps=[AssetKey("system_clean_up_dagster_files")])
def system_clean_up_dagster_runs(
    system_fetch_old_dagster_run_ids: list[str],
) -> list[str]:
    """Take ids of fetched old runs and clean them up."""
    instance = DagsterInstance.get()

    deleted_run_ids: list[str] = []
    for run_id in system_fetch_old_dagster_run_ids:
        instance.delete_run(run_id)
        deleted_run_ids.append(run_id)

    logger.info("Deleted %s old runs", len(deleted_run_ids))

    return deleted_run_ids


def _delete_asset_metadata(asset_key: AssetKey) -> None:
    """Delete obsolete Dagster assets and their metadata.

    For this we're using Dagsters officially documented `dagster asset wipe` command.
    """
    asset_key_json = json.dumps(asset_key.path)

    cmd = ["dagster", "asset", "wipe", asset_key_json, "--noprompt"]

    try:
        subprocess.run(  # noqa: S603 - command built from trusted literals and internally constructed AssetKey name
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.exception(
            "Failed to wipe Dagster metadata for asset key %s. stdout=%s stderr=%s",
            asset_key.to_user_string(),
            exc.stdout,
            exc.stderr,
        )
        msg = (
            f"Could not wipe Dagster metadata "
            f"for asset key {asset_key.to_user_string()}"
        )
        raise RuntimeError(msg) from exc


@asset(group_name="system_clean_up")
def system_clean_up_obsolete_assets() -> None:
    """Delete obsolete assets and their materialization.

    Compare current asset keys which are defined in the code, with all historic asset
    keys known to the dagster instance. The assets which are outdated (i.e. no longer
    defined in code) will be deleted along with their materializations.
    """
    definitions = load_job_definitions()
    current_asset_keys = set(definitions.resolve_asset_graph().get_all_asset_keys())

    instance = DagsterInstance.get()
    historical_asset_keys = set(instance.all_asset_keys())

    obsolete_asset_keys = historical_asset_keys - current_asset_keys

    settings = Settings.get()
    protected_prefixes = settings.system.protected_asset_prefixes

    deletion_candidates = [
        key
        for key in obsolete_asset_keys
        if not any(
            key.to_user_string().startswith(prefix) for prefix in protected_prefixes
        )
    ]

    for candidate in deletion_candidates:
        _delete_asset_metadata(candidate)

    logger.info("Deleted %s obsolete asset materializations.", len(deletion_candidates))

import datetime
import shutil
from pathlib import Path

from dagster import DagsterInstance, RunsFilter, asset
from pytz import UTC

from mex.common.logging import logger
from mex.extractors.settings import Settings


@asset(group_name="system_clean_up")
def system_clean_up_dagster_runs() -> list[str]:
    """Clean up dagster runs."""
    instance = DagsterInstance.get()
    settings = Settings.get()
    # Define the time threshold
    cutoff_date = datetime.datetime.now(tz=UTC) - datetime.timedelta(
        days=settings.system.max_run_age_in_days
    )

    # Get old runs
    old_run_records = instance.get_run_records(
        filters=RunsFilter(created_before=cutoff_date),
        limit=100,  # Process in batches
        ascending=True,
    )

    logger.info("Starting to delete %s runs.", len(old_run_records))

    # Delete run from database
    deleted_run_ids: list[str] = []

    for record in old_run_records:
        run_id = record.dagster_run.run_id
        instance.delete_run(run_id)
        deleted_run_ids.append(run_id)

    logger.info("Deleted %s runs", len(deleted_run_ids))

    return deleted_run_ids


@asset(group_name="system_clean_up")
def system_clean_up_dagster_files(system_clean_up_dagster_runs: list[str]) -> None:
    """Clean up dagster files."""
    instance = DagsterInstance.get()
    storage_path = Path(instance.storage_directory())
    logger.info("Storage Path: %s", storage_path)

    for run_id in system_clean_up_dagster_runs:
        run_storage_path = storage_path / run_id
        shutil.rmtree(run_storage_path)

    logger.info("Deleted %s folders of old runs.", len(system_clean_up_dagster_runs))

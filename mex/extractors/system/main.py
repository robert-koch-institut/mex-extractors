import datetime
import shutil
from pathlib import Path

from dagster import DagsterInstance, RunsFilter, asset
from pytz import UTC

from mex.common.logging import logger
from mex.extractors.settings import Settings


def system_fetch_old_dagster_run_ids() -> list[str]:
    """Fetch ids of Dagster runs, which are old enough to be deleted."""
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

    return [record.dagster_run.run_id for record in old_run_records]


@asset(group_name="system_clean_up")
def system_clean_up_dagster_files(
    system_fetch_old_dagster_run_ids: list[str],
) -> list[str]:
    """Clean up dagster files of old runs and return the id of the deleted files."""
    instance = DagsterInstance.get()
    storage_path = Path(instance.storage_directory())

    deleted_file_ids: list[str] = []

    for run_id in system_fetch_old_dagster_run_ids:
        run_storage_path = storage_path / run_id
        shutil.rmtree(run_storage_path)
        deleted_file_ids.append(run_id)

    logger.info(
        "Deleted %s folders of old runs from %s.",
        len(deleted_file_ids),
        storage_path,
    )

    return deleted_file_ids


@asset(group_name="system_clean_up")
def system_clean_up_dagster_runs(system_clean_up_dagster_files: list[str]) -> None:
    """Take ids of deleted dagster files and clean up the according dagster runs."""
    instance = DagsterInstance.get()

    deleted_run_ids: list[str] = []

    for run_id in system_clean_up_dagster_files:
        instance.delete_run(run_id)
        deleted_run_ids.append(run_id)

    logger.info("Deleted %s runs", len(deleted_run_ids))

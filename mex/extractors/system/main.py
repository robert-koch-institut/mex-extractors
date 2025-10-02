import datetime

from dagster import DagsterInstance, RunsFilter, asset
from pytz import UTC

from mex.extractors.settings import Settings


@asset(group_name="clean_up")
def dagster_clean_up_runs() -> list[str]:
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

    # Delete run from database
    deleted_run_ids: list[str] = []

    for record in old_run_records:
        run_id = record.dagster_run.run_id
        instance.delete_run(run_id)
        deleted_run_ids.append(run_id)

    return deleted_run_ids

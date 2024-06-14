from pathlib import Path

from mex.common.cli import entrypoint
from mex.common.settings import BaseSettings
from mex.pipeline import asset, run_job_in_process
from mex.publisher.extract import get_merged_items
from mex.publisher.load import write_merged_items
from mex.settings import Settings


@asset(group_name="publisher")
def publish_merged_items() -> None:
    """Get all merged items from mex-backend and write to ndjson file."""
    settings = BaseSettings.get()
    file_name = Path(settings.work_dir, "publisher.ndjson")

    open(file_name, "w").close()  # creates an empty file / empties existing file

    items = get_merged_items()

    write_merged_items(file_name, items)


@entrypoint(Settings)
def run() -> None:
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

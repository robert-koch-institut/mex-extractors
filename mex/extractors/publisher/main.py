from mex.common.cli import entrypoint
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.publisher.extract import get_merged_items
from mex.extractors.publisher.load import write_merged_items
from mex.extractors.settings import Settings


@asset(group_name="publisher")
def publish_merged_items() -> None:
    """Get all merged items from mex-backend and write to ndjson file."""
    items = get_merged_items()

    write_merged_items(items)


@entrypoint(Settings)
def run() -> None:
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

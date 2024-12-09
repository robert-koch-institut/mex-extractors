from mex.common.cli import entrypoint
from mex.common.models import AnyMergedModel
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.publisher.extract import get_merged_items
from mex.extractors.publisher.filter import filter_merged_items
from mex.extractors.publisher.load import write_merged_items
from mex.extractors.settings import Settings


@asset(group_name="publisher")
def extract_and_filter_merged_items() -> list[AnyMergedModel]:
    """Get merged items from mex-backend and filter them by allow-list."""
    items = get_merged_items()

    return list(filter_merged_items(items))


@asset(group_name="publisher")
def publish_merged_items(extract_and_filter_merged_items: list[AnyMergedModel]) -> None:
    """Write recieved merged items to ndjson file."""
    write_merged_items(extract_and_filter_merged_items)


@entrypoint(Settings)
def run() -> None:
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

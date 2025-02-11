from mex.common.cli import entrypoint
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.publisher.extract import get_merged_items
from mex.extractors.publisher.filter import filter_merged_items
from mex.extractors.publisher.load import write_merged_items
from mex.extractors.publisher.models import PublisherContainer
from mex.extractors.settings import Settings


@asset(group_name="publisher")
def extract_and_filter_merged_items() -> PublisherContainer:
    """Get merged items from mex-backend and filter them by allow-list."""
    items = get_merged_items()
    filtered = list(filter_merged_items(items))
    return PublisherContainer(items=filtered)


@asset(group_name="publisher")
def publish_merged_items(extract_and_filter_merged_items: PublisherContainer) -> None:
    """Write received merged items to ndjson file."""
    write_merged_items(extract_and_filter_merged_items.items)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

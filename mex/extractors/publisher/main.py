from mex.common.backend_api.models import MergedItemsResponse
from mex.common.cli import entrypoint
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.publisher.extract import get_merged_items
from mex.extractors.publisher.filter import filter_merged_items
from mex.extractors.publisher.load import write_merged_items
from mex.extractors.settings import Settings


@asset(group_name="publisher")
def extract_and_filter_merged_items() -> MergedItemsResponse:
    """Get merged items from mex-backend and filter them by allow-list."""
    items = get_merged_items()
    filtered = list(filter_merged_items(items))
    return MergedItemsResponse(items=filtered, total=len(filtered))


@asset(group_name="publisher")
def publish_merged_items(extract_and_filter_merged_items: MergedItemsResponse) -> None:
    """Write received merged items to ndjson file."""
    write_merged_items(extract_and_filter_merged_items.items)


@entrypoint(Settings)
def run() -> None:
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

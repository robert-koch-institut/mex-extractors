from mex.common.cli import entrypoint
from mex.common.models import AnyMergedModel, ItemsContainer
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.publisher.extract import get_merged_items
from mex.extractors.publisher.filter import filter_merged_items
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink


@asset(group_name="publisher")
def extract_and_filter_merged_items() -> ItemsContainer[AnyMergedModel]:
    """Get merged items from mex-backend and filter them by allow-list."""
    items = get_merged_items()
    filtered = list(filter_merged_items(items))
    return ItemsContainer[AnyMergedModel](items=filtered)


@asset(group_name="publisher")
def publish_merged_items(
    extract_and_filter_merged_items: ItemsContainer[AnyMergedModel],
) -> None:
    """Write received merged items to configured sink."""
    s3 = S3Sink.get()
    s3.load(extract_and_filter_merged_items.items)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

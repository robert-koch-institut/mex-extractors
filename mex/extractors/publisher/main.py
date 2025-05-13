from collections import deque

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import AnyMergedModel, ItemsContainer
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.extract import get_merged_items
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink


@asset(group_name="publisher")
def extract_and_filter_merged_items() -> ItemsContainer[AnyMergedModel]:
    """Get merged items from mex-backend and filter them by allow-list."""
    merged_items = list(get_merged_items())
    return ItemsContainer[AnyMergedModel](items=merged_items)


@asset(group_name="publisher")
def publish_merged_items(
    extract_and_filter_merged_items: ItemsContainer[AnyMergedModel],
) -> None:
    """Write received merged items to configured sink."""
    s3 = S3Sink.get()
    deque(s3.load(extract_and_filter_merged_items.items), maxlen=0)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

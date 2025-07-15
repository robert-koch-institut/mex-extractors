from collections import deque
from itertools import chain

from dagster import asset

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.cli import entrypoint
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    ItemsContainer,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.extract import get_publishable_merged_items
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink


@asset(group_name="publisher")
def publishable_merged_items() -> ItemsContainer[AnyMergedModel]:
    """Get merged items from mex-backend and filter them by allow-list."""
    settings = Settings.get()
    allowed_entity_types = [
        item
        for item in MERGED_MODEL_CLASSES_BY_NAME
        if item not in settings.publisher.skip_merged_items
    ]
    merged_items = get_publishable_merged_items(
        entity_type=allowed_entity_types,
    )
    return ItemsContainer[AnyMergedModel](items=merged_items)


@asset(group_name="publisher")
def publishable_merged_persons() -> ItemsContainer[AnyMergedModel]:
    """Get merged persons from green-lit primary sources.."""
    settings = Settings.get()
    connector = BackendApiConnector.get()
    allowed_primary_sources = sorted(
        {
            str(primary_source.stableTargetId)
            for primary_source in connector.fetch_extracted_items(
                None, None, ["ExtractedPrimarySource"], 0, 100
            ).items
            if primary_source.identifierInPrimarySource
            in settings.publisher.allowed_person_primary_sources
        }
    )
    merged_items = get_publishable_merged_items(
        had_primary_source=allowed_primary_sources,
        entity_type=["MergedPerson"],
    )
    return ItemsContainer[AnyMergedModel](items=merged_items)


@asset(group_name="publisher")
def publish_merged_items(
    publishable_merged_items: ItemsContainer[AnyMergedModel],
    publishable_merged_persons: ItemsContainer[AnyMergedModel],
) -> None:
    """Write received merged items to configured sink."""
    s3 = S3Sink.get()
    items = chain(publishable_merged_items.items, publishable_merged_persons.items)
    deque(s3.load(items), maxlen=0)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

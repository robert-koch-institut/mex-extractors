from collections import deque
from typing import cast

from dagster import asset

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.cli import entrypoint
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyMergedModel,
    ItemsContainer,
    MergedContactPoint,
    PaginatedItemsContainer,
)
from mex.common.types import MergedContactPointIdentifier
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.extract import get_publishable_merged_items
from mex.extractors.publisher.transform import update_actor_references_where_needed
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink


@asset(group_name="publisher")
def publishable_items_without_actors() -> ItemsContainer[AnyMergedModel]:
    """All items with entity types that are neither an actor nor skipped.

    Actor types are: Person, ContactPoint and OrganizationalUnit.
    """
    settings = Settings.get()
    allowed_entity_types = [
        entity_type
        for entity_type in MERGED_MODEL_CLASSES_BY_NAME
        if entity_type
        not in [
            *settings.publisher.skip_entity_types,
            "MergedPerson",
            "MergedContactPoint",
            "MergedOrganizationalUnit",
        ]
    ]
    merged_items = get_publishable_merged_items(
        entity_type=allowed_entity_types,
    )
    return ItemsContainer[AnyMergedModel](items=merged_items)


@asset(group_name="publisher")
def publishable_persons() -> ItemsContainer[AnyMergedModel]:
    """Get publishable persons from green-lit primary sources."""
    settings = Settings.get()
    connector = BackendApiConnector.get()
    limit = 100
    primary_sources = connector.fetch_extracted_items(
        None, None, ["ExtractedPrimarySource"], 0, limit
    )
    if primary_sources.total > limit:
        raise NotImplementedError
    allowed_primary_sources = sorted(
        {
            str(primary_source.stableTargetId)
            for primary_source in primary_sources.items
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
def publishable_contact_points_and_units() -> ItemsContainer[AnyMergedModel]:
    """Get publishable contact points and organizational units."""
    settings = Settings.get()
    allowed_entity_types = [
        entity_type
        for entity_type in MERGED_MODEL_CLASSES_BY_NAME
        if entity_type in ["MergedContactPoint", "MergedOrganizationalUnit"]
        and entity_type not in settings.publisher.skip_entity_types
    ]
    merged_items = get_publishable_merged_items(
        entity_type=allowed_entity_types,
    )
    return ItemsContainer[AnyMergedModel](items=merged_items)


@asset(group_name="publisher")
def fallback_contact_identifiers() -> list[MergedContactPointIdentifier]:
    """Get the mex contact point as a fallback contact."""
    settings = Settings.get()
    connector = BackendApiConnector.get()
    response = cast(
        "PaginatedItemsContainer[MergedContactPoint]",
        connector.fetch_merged_items(
            str(settings.contact_point.mex_email),
            ["MergedContactPoint"],
            [MEX_PRIMARY_SOURCE_STABLE_TARGET_ID],
            0,
            1,
        ),
    )
    return [item.identifier for item in response.items]


@asset(group_name="publisher")
def publishable_items(
    publishable_items_without_actors: ItemsContainer[AnyMergedModel],
    publishable_persons: ItemsContainer[AnyMergedModel],
    publishable_contact_points_and_units: ItemsContainer[AnyMergedModel],
    fallback_contact_identifiers: list[MergedContactPointIdentifier],
) -> ItemsContainer[AnyMergedModel]:
    """All publishable items with updated contact references, where needed."""
    allowed_actors = {
        person.identifier
        for person in publishable_persons.items
        + publishable_contact_points_and_units.items
    }
    for item in publishable_items_without_actors.items:
        update_actor_references_where_needed(
            item, allowed_actors, fallback_contact_identifiers
        )
    return ItemsContainer[AnyMergedModel](
        items=publishable_items_without_actors.items
        + publishable_persons.items
        + publishable_contact_points_and_units.items
    )


@asset(group_name="publisher")
def publish_to_s3(publishable_items: ItemsContainer[AnyMergedModel]) -> None:
    """Write received merged items to s3 sink."""
    s3 = S3Sink.get()
    deque(s3.load(publishable_items.items), maxlen=0)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

from collections import deque
from typing import cast

from dagster import asset

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.cli import entrypoint
from mex.common.exceptions import EmptySearchResultError, FoundMoreThanOneError
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyMergedModel,
    ItemsContainer,
    MergedContactPoint,
    PaginatedItemsContainer,
)
from mex.common.types import MergedContactPointIdentifier
from mex.extractors.contact_point.main import MEX_EMAIL
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.extract import get_publishable_merged_items
from mex.extractors.publisher.transform import update_contact_where_needed
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink

CONTACT_ENTITY_TYPES = [
    "MergedPerson",
    "MergedContactPoint",
    "MergedOrganizationalUnit",
]


@asset(group_name="publisher")
def publishable_items_without_contacts() -> ItemsContainer[AnyMergedModel]:
    """All items with entity types that are neither a contact nor skipped."""
    settings = Settings.get()
    allowed_entity_types = [
        entity_type
        for entity_type in MERGED_MODEL_CLASSES_BY_NAME
        if entity_type not in settings.publisher.skip_entity_types
        and entity_type not in CONTACT_ENTITY_TYPES
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
def publishable_contacts_without_persons() -> ItemsContainer[AnyMergedModel]:
    """All items with contact entity types except Persons: so ContactPoint and Unit."""
    settings = Settings.get()
    allowed_entity_types = [
        entity_type
        for entity_type in MERGED_MODEL_CLASSES_BY_NAME
        if entity_type in CONTACT_ENTITY_TYPES
        and entity_type not in settings.publisher.skip_entity_types
    ]
    merged_items = get_publishable_merged_items(
        entity_type=allowed_entity_types,
    )
    return ItemsContainer[AnyMergedModel](items=merged_items)


@asset(group_name="publisher")
def mex_contact_point_identifier() -> MergedContactPointIdentifier:
    """Get the mex contact point from the backend."""
    connector = BackendApiConnector.get()
    response = cast(
        "PaginatedItemsContainer[MergedContactPoint]",
        connector.fetch_merged_items(
            MEX_EMAIL,
            ["MergedContactPoint"],
            [MEX_PRIMARY_SOURCE_STABLE_TARGET_ID],
            0,
            1,
        ),
    )
    if response.total == 0:
        msg = f"No ContactPoint for {MEX_EMAIL} found."
        raise EmptySearchResultError(msg)
    if response.total >= 1:
        msg = f"Found {response.total} ContactPoints for {MEX_EMAIL}, expected 1."
        raise FoundMoreThanOneError(msg)
    return response.items[0].identifier


@asset(group_name="publisher")
def publishable_items(
    publishable_items_without_contacts: ItemsContainer[AnyMergedModel],
    publishable_merged_persons: ItemsContainer[AnyMergedModel],
    publishable_contacts_without_persons: ItemsContainer[AnyMergedModel],
    mex_contact_point_identifier: MergedContactPointIdentifier,
) -> ItemsContainer[AnyMergedModel]:
    """All publishable items with updated contact references, where needed."""
    allowed_contacts = {
        person.identifier
        for person in publishable_merged_persons.items
        + publishable_contacts_without_persons.items
    }
    fallback_contacts = [
        mex_contact_point_identifier,
    ]
    for item in publishable_items_without_contacts.items:
        update_contact_where_needed(item, allowed_contacts, fallback_contacts)
    return ItemsContainer[AnyMergedModel](
        items=publishable_items_without_contacts.items
        + publishable_merged_persons.items
        + publishable_contacts_without_persons.items
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

from collections import deque
from typing import cast

from dagster import asset

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.cli import entrypoint
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ItemsContainer,
    MergedConsent,
    MergedContactPoint,
    MergedPerson,
    PaginatedItemsContainer,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.publisher.extract import get_publishable_merged_items
from mex.extractors.publisher.filter import (
    filter_persons_with_appoving_unique_consent,
)
from mex.extractors.publisher.transform import (
    get_unit_id_per_person,
    update_actor_references_where_needed,
)
from mex.extractors.publisher.types import PublisherItemsLike
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink


@asset(group_name="publisher")
def publisher_items_without_actors() -> PublisherItemsLike:
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
    merged_items = get_publishable_merged_items(entity_type=allowed_entity_types)
    return ItemsContainer(items=merged_items)


@asset(group_name="publisher")
def publisher_merged_persons() -> list[MergedPerson]:
    """Fetch all MergedPersons with Primary source = ldap."""
    return cast(
        "list[MergedPerson]",
        get_publishable_merged_items(
            entity_type=["MergedPerson"],
            referenced_identifier=[
                str(get_extracted_primary_source_id_by_name("ldap"))
            ],
            reference_field="hadPrimarySource",
        ),
    )


@asset(group_name="publisher")
def publisher_persons() -> PublisherItemsLike:
    """Get publishable persons with positive consent."""
    merged_persons = cast(
        "list[MergedPerson]",
        get_publishable_merged_items(entity_type=["MergedPerson"]),
    )
    merged_consent = cast(
        "list[MergedConsent]",
        get_publishable_merged_items(entity_type=["MergedConsent"]),
    )
    filtered_persons = filter_persons_with_appoving_unique_consent(
        merged_persons, merged_consent
    )
    return ItemsContainer(items=filtered_persons)


@asset(group_name="publisher")
def publisher_contact_points_and_units() -> PublisherItemsLike:
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
    return ItemsContainer(items=merged_items)


@asset(group_name="publisher")
def publisher_fallback_contact_identifiers() -> list[MergedContactPointIdentifier]:
    """Get the mex contact point as a fallback contact."""
    settings = Settings.get()
    connector = BackendApiConnector.get()
    response = cast(
        "PaginatedItemsContainer[MergedContactPoint]",
        connector.fetch_merged_items(
            query_string=str(settings.contact_point.mex_email),
            entity_type=["MergedContactPoint"],
            referenced_identifier=[MEX_PRIMARY_SOURCE_STABLE_TARGET_ID],
            reference_field="hadPrimarySource",
            limit=1,
        ),
    )
    return [item.identifier for item in response.items]


@asset(group_name="publisher")
def publisher_fallback_unit_identifiers_by_person(
    publisher_merged_persons: list[MergedPerson],
    publisher_contact_points_and_units: PublisherItemsLike,
) -> dict[MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]]:
    """For each Person get their unit IDs if the unit has an email address."""
    return get_unit_id_per_person(
        publisher_merged_persons,
        publisher_contact_points_and_units,
    )


@asset(group_name="publisher")
def publisher_items(
    publisher_items_without_actors: PublisherItemsLike,
    publisher_persons: PublisherItemsLike,
    publisher_contact_points_and_units: PublisherItemsLike,
    publisher_fallback_contact_identifiers: list[MergedContactPointIdentifier],
    publisher_fallback_unit_identifiers_by_person: dict[
        MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]
    ],
) -> PublisherItemsLike:
    """All publishable items with updated contact references, where needed."""
    allowed_actors = {
        person.identifier
        for person in publisher_persons.items + publisher_contact_points_and_units.items
    }
    for item in publisher_items_without_actors.items:
        update_actor_references_where_needed(
            item,
            allowed_actors,
            publisher_fallback_contact_identifiers,
            publisher_fallback_unit_identifiers_by_person,
        )
    return ItemsContainer(
        items=publisher_items_without_actors.items
        + publisher_persons.items
        + publisher_contact_points_and_units.items
    )


@asset(group_name="publisher")
def publisher_s3_load(publisher_items: PublisherItemsLike) -> None:
    """Write received merged items to s3 sink."""
    s3 = S3Sink.get()
    deque(s3.load(publisher_items.items), maxlen=0)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

from collections import deque
from typing import TYPE_CHECKING, cast

from dagster import asset

from mex.common.backend_api.connector import ReferenceFilter
from mex.common.cli import entrypoint
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ItemsContainer,
    MergedConsent,
    MergedContactPoint,
    MergedPerson,
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
    cluster_and_filter_bibliographic_resources_by_unit,
    filter_persons_with_approving_unique_consent,
)
from mex.extractors.publisher.models import (
    BibliographicResourceForCsv,
    PublisherItemsLike,
)
from mex.extractors.publisher.transform import (
    get_unit_id_per_person,
    transform_merged_bibliographic_resources_for_csv,
    update_actor_references_where_needed,
)
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks.ndjson import NdjsonSink
from mex.extractors.sinks.s3 import S3CsvSink, S3Sink

if TYPE_CHECKING:
    from mex.common.models import MergedBibliographicResource


@asset(group_name="publisher")
def publisher_items_without_actors() -> PublisherItemsLike:
    """Get all items with entity types that are neither an actor nor skipped.

    Actor types are: Person, ContactPoint and OrganizationalUnit. These are fetched and
    handled specially at a later point.
    Settings:
        publisher.skip_entity_types: entity type to skip on top of actor types.
    """
    settings = ExtractorsSettings.get()
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
def publisher_merged_ldap_persons() -> list[MergedPerson]:
    """Fetch all MergedPersons with Primary source = ldap."""
    return cast(
        "list[MergedPerson]",
        get_publishable_merged_items(
            entity_type=["MergedPerson"],
            reference_filters=[
                ReferenceFilter(
                    field="hadPrimarySource",
                    identifiers=[str(get_extracted_primary_source_id_by_name("ldap"))],
                )
            ],
        ),
    )


@asset(group_name="publisher")
def publisher_persons() -> PublisherItemsLike:
    """Get publishable persons with exactly 1 consent which is approving."""
    merged_persons = cast(
        "list[MergedPerson]",
        get_publishable_merged_items(entity_type=["MergedPerson"]),
    )
    merged_consent = cast(
        "list[MergedConsent]",
        get_publishable_merged_items(entity_type=["MergedConsent"]),
    )
    filtered_persons = filter_persons_with_approving_unique_consent(
        merged_persons, merged_consent
    )
    return ItemsContainer(items=filtered_persons)


@asset(group_name="publisher")
def publisher_contact_points_and_units() -> PublisherItemsLike:
    """Get publishable contact points and organizational units."""
    settings = ExtractorsSettings.get()
    allowed_entity_types = [
        entity_type
        for entity_type in ["MergedContactPoint", "MergedOrganizationalUnit"]
        if entity_type not in settings.publisher.skip_entity_types
    ]
    merged_items = get_publishable_merged_items(
        entity_type=allowed_entity_types,
    )
    return ItemsContainer(items=merged_items)


@asset(group_name="publisher")
def publisher_fallback_contact_identifiers() -> list[MergedContactPointIdentifier]:
    """Get the mex contact point as a fallback contact."""
    settings = ExtractorsSettings.get()
    merged_contact_points = cast(
        "list[MergedContactPoint]",
        get_publishable_merged_items(
            query_string=str(settings.contact_point.mex_email),
            entity_type=["MergedContactPoint"],
            reference_filters=[
                ReferenceFilter(
                    field="hadPrimarySource",
                    identifiers=[MEX_PRIMARY_SOURCE_STABLE_TARGET_ID],
                )
            ],
        ),
    )
    return [merged_contact_points[0].identifier]


@asset(group_name="publisher")
def publisher_fallback_unit_identifiers_by_person(
    publisher_merged_ldap_persons: list[MergedPerson],
    publisher_contact_points_and_units: PublisherItemsLike,
) -> dict[MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]]:
    """For each Person get their unit IDs if the unit has an email address."""
    return get_unit_id_per_person(
        publisher_merged_ldap_persons,
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
    """All publishable items with updated person/contact references, where needed."""
    allowed_actors = {
        actor.identifier
        for actor in publisher_persons.items + publisher_contact_points_and_units.items
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
def publisher_sink_load(publisher_items: PublisherItemsLike) -> None:
    """Write received merged items to the configured sink."""
    settings = ExtractorsSettings.get()
    sink: S3Sink | NdjsonSink
    if settings.publisher.sink == "s3":
        sink = S3Sink.get()
    else:
        sink = NdjsonSink.get()

    deque(sink.load(publisher_items.items), maxlen=0)


@asset(group_name="publisher")
def publisher_bibliographic_resources_for_csv_by_unit() -> dict[
    str, list[BibliographicResourceForCsv]
]:
    """Extract the bibliographic resources and transform to format for the csv list."""
    merged_bibliographic_resources = cast(
        "list[MergedBibliographicResource]",
        get_publishable_merged_items(entity_type=["MergedBibliographicResource"]),
    )
    merged_bibliographic_resources_by_unit = (
        cluster_and_filter_bibliographic_resources_by_unit(
            merged_bibliographic_resources
        )
    )
    return transform_merged_bibliographic_resources_for_csv(
        merged_bibliographic_resources_by_unit
    )


@asset(group_name="publisher")
def publisher_csv_load(
    publisher_bibliographic_resources_for_csv_by_unit: dict[
        str, list[BibliographicResourceForCsv]
    ],
) -> None:
    """Write BibliographicResourceForCsv as CSV to s3 sink."""
    s3csv = S3CsvSink()
    for (
        unit_name,
        publications,
    ) in publisher_bibliographic_resources_for_csv_by_unit.items():
        publications_sorted_by_year = sorted(
            publications,
            key=lambda item: (
                item.publicationYear is not None,
                item.publicationYear or 0,
            ),
            reverse=True,
        )
        deque(
            s3csv.load_for_unit(publications_sorted_by_year, unit_name=unit_name),
            maxlen=0,
        )


@entrypoint()
def run() -> None:  # pragma: no cover
    """Run the publisher job in-process."""
    run_job_in_process("publisher")

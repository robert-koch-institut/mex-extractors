from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.models import (
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
)
from mex.common.types import (
    AnyMergedIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
)
from mex.extractors.publisher.extract import get_publishable_merged_item_by_identifier
from mex.extractors.publisher.fields import (
    REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME,
)
from mex.extractors.publisher.models import BibliographicResourceForCsv

if TYPE_CHECKING:
    from collections.abc import Collection

    from mex.common.models import (
        AnyMergedModel,
        MergedBibliographicResource,
    )
    from mex.extractors.publisher.models import PublisherItemsLike


def get_unit_id_per_person(
    publisher_merged_ldap_persons: list[MergedPerson],
    publisher_contact_points_and_units: PublisherItemsLike,
) -> dict[MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]]:
    """For each Person get their unit IDs if the unit has an email address.

    Args:
        publisher_merged_ldap_persons: Merged Persons with primary source ldap
        publisher_contact_points_and_units: Items container of units + contact points

    Returns:
        dictionary of unit identifiers by person identifier
    """
    unit_id_if_email = {
        unit.identifier
        for unit in publisher_contact_points_and_units.items
        if (
            isinstance(unit.identifier, MergedOrganizationalUnitIdentifier)
            and unit.email
        )
    }

    return {
        person.identifier: [
            unit_id for unit_id in person.memberOf if unit_id in unit_id_if_email
        ]
        for person in publisher_merged_ldap_persons
    }


def update_actor_references_where_needed(
    item: AnyMergedModel,
    allowed_actors: Collection[AnyMergedIdentifier],
    fallback_contact_identifiers: list[MergedContactPointIdentifier],
    fallback_unit_identifiers_by_person: dict[
        MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]
    ],
) -> None:
    """Update references to actors, where needed.

    We filter all fields that allow Person references to only contain references to
    publishable actors. For fields that also allow organizational units,
    non-consenting persons can get replaced by their organizational unit if the unit
    provides an email address. Fields that allow contact points, but contain no valid
    references are set to a fallback contact point.
    Should the field be required, not allow contact points, but still contain no valid
    references, we keep the broken ones in order to keep mex-model compliance.
    Would we skip those items instead, we might break other items relying on the former
    item, and start a recursive de-publication process - which we don't want.
    """
    for field, ref_types in REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME[
        item.entityType
    ].items():
        if "MergedPerson" not in ref_types:
            continue

        # keep allowed actors (contact points and units)
        allowed_field_identifiers = [
            identifier
            for identifier in getattr(item, field)
            if identifier in allowed_actors
        ]

        # replace unpublishable persons with their unit id if unit has email
        replacement_field_identifiers: set[MergedOrganizationalUnitIdentifier] = set()
        if "MergedOrganizationalUnit" in ref_types:
            replacement_field_identifiers = {
                unit_id
                for identifier in getattr(item, field)
                if identifier not in allowed_actors
                for unit_id in fallback_unit_identifiers_by_person.get(identifier, [])
                if unit_id in allowed_actors
            }

        identifiers = allowed_field_identifiers + sorted(replacement_field_identifiers)
        # if there is still no allowed actor: set to fallback contact point, if possible
        if not identifiers and "MergedContactPoint" in ref_types:
            identifiers = fallback_contact_identifiers
        if not identifiers and item.model_fields[field].is_required():
            logger.error(
                "%s(identifier='%s') has no valid references "
                "for required field %s, publishing broken references",
                item.entityType,
                item.identifier,
                field,
            )
        else:
            setattr(item, field, identifiers)


def get_resolved_names(
    identifier: AnyMergedIdentifier, entitiy_type: type[AnyMergedModel], field: str
) -> str | None:
    """Get names from specified fields or referenced merged items.

    Args:
        identifier: identifier of referenced merged item of which to get the name.
        entitiy_type: entity type of the merged item.
        field: field of the merged item in which the name needs to be looked up.

    Returns:
        name as string or None if not found.

    Raises:
        MExError if name or merged item have the wrong type.
    """
    merged_item = get_publishable_merged_item_by_identifier(identifier)

    if not isinstance(merged_item, entitiy_type):
        msg = "Wrong entity type given for referenced merged item."
        raise MExError(msg)

    name_list = getattr(merged_item, field, None)

    if not name_list:
        return None

    name = name_list[0]

    if isinstance(name, Text):
        return name.value
    if isinstance(name, str):
        return name

    msg = "Looked up name is neither string nor text."
    raise MExError(msg)


def transform_merged_bibliographic_resources_for_csv(
    merged_bibliographic_resources: list[MergedBibliographicResource],
) -> list[BibliographicResourceForCsv]:
    """Transform merged bibliographic resources to bibliographic resources for csv.

    Args:
        merged_bibliographic_resources: list of merged bibliographic resources.

    Returns:
        list of BibliographicResourceForCsv entries.
    """
    bibliographic_resources_for_csv: list[BibliographicResourceForCsv] = []
    for bibliographic_resource in merged_bibliographic_resources:
        contributing_unit = [
            get_resolved_names(unit, MergedOrganizationalUnit, "shortName")
            for unit in bibliographic_resource.contributingUnit
        ]
        creator = [
            get_resolved_names(person, MergedPerson, "fullName")
            for person in bibliographic_resource.creator
        ]
        title = [title.value for title in bibliographic_resource.title]
        journal = [journal.value for journal in bibliographic_resource.journal]
        access_restriction = (
            type(bibliographic_resource.accessRestriction).__concepts__[0].prefLabel.de
        )
        publisher = [
            get_resolved_names(publisher, MergedOrganization, "officialName")
            for publisher in bibliographic_resource.publisher
        ]

        bibliographic_resources_for_csv.append(
            BibliographicResourceForCsv(
                contributingUnit=contributing_unit,
                publicationYear=bibliographic_resource.publicationYear,
                creator=creator,
                title=title,
                journal=journal,
                doi=bibliographic_resource.doi,
                accessRestriction=access_restriction,
                publisher=publisher,
            )
        )

    return bibliographic_resources_for_csv

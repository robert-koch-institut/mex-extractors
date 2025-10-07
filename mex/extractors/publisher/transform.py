from collections.abc import Collection

from mex.common.logging import logger
from mex.common.models import (
    AnyMergedModel,
    ItemsContainer,
    MergedPerson,
)
from mex.common.types import (
    AnyMergedIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.publisher.fields import (
    REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME,
)


def get_unit_id_per_person(
    publisher_merged_ldap_persons: list[MergedPerson],
    publisher_contact_points_and_units: ItemsContainer[AnyMergedModel],
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

        # replace un-allowed persons with their unit id if unit has email
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

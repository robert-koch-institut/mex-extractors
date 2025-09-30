from collections import defaultdict
from collections.abc import Collection
from itertools import chain

from mex.common.logging import logger
from mex.common.models import AnyMergedModel, MergedOrganizationalUnit, MergedPerson
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
    merged_ldap_persons: list[MergedPerson],
    publishable_organizational_units: list[MergedOrganizationalUnit],
) -> dict[MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]]:
    """For each Person get their unit IDs if the unit has an email address.

    Args:
        merged_ldap_persons: Merged Persons with primary source ldap
        publishable_organizational_units: Merged organizational units

    Returns:
        dictionary of unit identifiers by person identifier

    # result = defaultdict(list)
    # for person in merged_ldap_persons:
    #     for unit in person.memberOf:
    #         if unit in unit_id_if_email:
    #             result[person.identifier].append(unit)

    # {
    #     person.identifier: unit
    #     for person in merged_ldap_persons
    #     for unit in person.memberOf
    #     if unit in unit_id_if_email
    # }
    """
    unit_id_if_email = {
        unit.identifier for unit in publishable_organizational_units if unit.email
    }

    unit_id_per_person_id = defaultdict(list)
    for person in merged_ldap_persons:
        for unit_id in person.memberOf:
            if unit_id in unit_id_if_email:
                unit_id_per_person_id[person.identifier].append(unit_id)

    return unit_id_per_person_id


def update_actor_references_where_needed(
    item: AnyMergedModel,
    allowed_actors: Collection[AnyMergedIdentifier],
    fallback_contact_identifiers: list[MergedContactPointIdentifier],
    fallback_unit_identifiers_by_person: dict[
        MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]
    ],
) -> None:
    """Update references to actors, where needed.

    We filter all fields (that allow Person references) to only contain references to
    publishable actors. Fields that allow contact points, but contain no valid
    references are set to a fallback contact point.
    Should the field be required, not allow contact points, but still contain no valid
    references, we keep the broken ones in order to keep mex-model compliance.
    Would we skip those items instead, we might break other items relying on the former
    item, and start a recursive de-publication process - which we don't want.

    fallback_unit_identifiers: list[MergedOrganizationalUnitIdentifier] = []
        if "MergedOrganizationalUnit" in ref_types:
            for identifier in getattr(item, field):
                if identifier in fallback_unit_identifiers_by_person:
                    fallback_unit_identifiers.extend(
                        fallback_unit_identifiers_by_person[identifier]
                    )
        allowed_actors_lookup = allowed_actors
        if fallback_unit_identifiers:
            allowed_actors_lookup = set(allowed_actors).union(
                fallback_unit_identifiers
            )
    """
    for field, ref_types in REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME[
        item.entityType
    ].items():
        if "MergedPerson" in ref_types:
            seen = set()
            identifiers = []
            for identifier in getattr(item, field):
                #breakpoint()
                if identifier in allowed_actors and identifier not in seen:
                    seen.add(identifier)
                    identifiers.append(identifier)
                elif "MergedOrganizationalUnit" in ref_types:
                    unit_ids = fallback_unit_identifiers_by_person.get(identifier, [])
                    for id in unit_ids:
                        if id in allowed_actors and id not in seen:
                            seen.add(id)
                            identifiers.append(id)

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

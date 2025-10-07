from collections.abc import Collection

from mex.common.logging import logger
from mex.common.models import AnyMergedModel
from mex.common.types import AnyMergedIdentifier, MergedContactPointIdentifier
from mex.extractors.publisher.fields import (
    REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME,
)


def update_actor_references_where_needed(
    item: AnyMergedModel,
    allowed_actors: Collection[AnyMergedIdentifier],
    publisher_fallback_contact_identifiers: list[MergedContactPointIdentifier],
) -> None:
    """Update references to actors, where needed.

    We filter all fields that allow Person references to only contain references to
    publishable actors. Fields that allow contact points, but contain no valid
    references are set to a fallback contact point.
    Should the field be required, not allow contact points, but still contain no valid
    references, we keep the broken ones in order to keep mex-model compliance.
    Would we skip those items instead, we might break other items relying on the former
    item, and start a recursive de-publication process - which we don't want.
    """
    for field, ref_types in REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME[
        item.entityType
    ].items():
        if "MergedPerson" in ref_types:
            identifiers = [
                identifier
                for identifier in getattr(item, field)
                if identifier in allowed_actors
            ]
            if not identifiers and "MergedContactPoint" in ref_types:
                identifiers = publisher_fallback_contact_identifiers
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

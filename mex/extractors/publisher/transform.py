from collections.abc import Collection, Iterable
from typing import cast

from mex.common.models import AnyMergedModel
from mex.common.types import AnyMergedIdentifier
from mex.extractors.publisher.fields import CONTACT_FIELDS_BY_CLASS_NAME
from mex.extractors.publisher.utils import ensure_list


def update_contact_where_needed(
    item: AnyMergedModel,
    allowed_contacts: Collection[AnyMergedIdentifier],
    fallback_contacts: Iterable[AnyMergedIdentifier],
) -> None:
    """Update references in contact fields, where needed."""
    fields_allowing_persons = CONTACT_FIELDS_BY_CLASS_NAME[item.entityType]
    for field in fields_allowing_persons:
        contacts = [
            reference
            for reference in cast(
                "list[AnyMergedIdentifier]", ensure_list(getattr(item, field))
            )
            if reference in allowed_contacts
        ]
        if not contacts and item.model_fields[field].is_required():
            contacts = list(fallback_contacts)
        setattr(item, field, contacts)

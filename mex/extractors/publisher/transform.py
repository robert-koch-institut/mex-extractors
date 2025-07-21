from collections.abc import Collection, Iterable

from mex.common.models import (
    AnyMergedModel,
    MergedAccessPlatform,
    MergedActivity,
    MergedPrimarySource,
    MergedResource,
)
from mex.common.types import AnyMergedIdentifier, MergedContactPointIdentifier

MergedModelsWithContact = (
    MergedAccessPlatform | MergedActivity | MergedPrimarySource | MergedResource
)


def update_contact_where_needed(
    item: AnyMergedModel,
    allowed_contacts: Collection[AnyMergedIdentifier],
    fallback_contact_identifiers: Iterable[MergedContactPointIdentifier],
) -> None:
    """Update references in contact fields, where needed."""
    if isinstance(item, MergedModelsWithContact):
        contacts = [
            reference for reference in item.contact if reference in allowed_contacts
        ]
        if not contacts and item.model_fields["contact"].is_required():
            contacts = list(fallback_contact_identifiers)
        item.contact = contacts

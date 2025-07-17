from functools import lru_cache
from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import EmptySearchResultError, FoundMoreThanOneError
from mex.common.fields import ALL_MODEL_CLASSES_BY_NAME
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyMergedModel,
    MergedContactPoint,
    MergedPerson,
)
from mex.common.types import (
    AnyMergedIdentifier,
    MergedContactPointIdentifier,
    MergedPersonIdentifier,
)
from mex.common.utils import contains_any_types, group_fields_by_class_name
from mex.extractors.contact_point.main import MEX_EMAIL

# lists of fields that can be person as well as contact point, grouped by class names
CONTACT_FIELDS_BY_CLASS_NAME = group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: contains_any_types(field_info, MergedPersonIdentifier)
    and contains_any_types(field_info, MergedContactPointIdentifier),
)


# TODO(ND): move this to mex-common
def ensure_list(values: object) -> list[object]:
    """Wrap single objects in lists, replace None with [] and return lists untouched."""
    if values is None:
        return []
    if isinstance(values, list):
        return values
    return [values]


@lru_cache(maxsize=1)
def get_mex_contact_point_identifier() -> MergedContactPointIdentifier:
    """Get the identifier of the mex contact point from the backend."""
    connector = BackendApiConnector.get()
    response = connector.fetch_merged_items(
        MEX_EMAIL,
        ["MergedContactPoint"],
        [MEX_PRIMARY_SOURCE_STABLE_TARGET_ID],
        0,
        1,
    )
    if response.total == 0:
        msg = f"No ContactPoint for {MEX_EMAIL} found."
        raise EmptySearchResultError(msg)
    if response.total >= 1:
        msg = f"Found {response.total} ContactPoints for {MEX_EMAIL} found, expected 1."
        raise FoundMoreThanOneError(msg)
    return cast("MergedContactPoint", response.items[0]).identifier


def filter_person_references(merged_model: AnyMergedModel) -> AnyMergedModel:
    """Remove references to Person items.

    If field is required and empty after filtering, it is set to the mex contact point.

    Args:
        merged_model: Merged model to check for person references.

    Returns:
        Merged Model without person references.
    """
    contact_point_identifier = get_mex_contact_point_identifier()
    connector = BackendApiConnector.get()
    fields_allowing_persons = CONTACT_FIELDS_BY_CLASS_NAME[merged_model.entityType]
    for field in fields_allowing_persons:
        references = cast(
            "list[AnyMergedIdentifier]", ensure_list(getattr(merged_model, field))
        )
        filtered_references: list[AnyMergedIdentifier] = []
        for reference in references:
            referenced_item = connector.get_merged_item(reference)
            if not isinstance(referenced_item, MergedPerson):
                filtered_references.append(reference)
        if not filtered_references and merged_model.model_fields[field].is_required():
            setattr(merged_model, field, contact_point_identifier)
        else:
            setattr(merged_model, field, filtered_references)
    return merged_model

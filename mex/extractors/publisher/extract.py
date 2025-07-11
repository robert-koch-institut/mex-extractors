from collections.abc import Generator
from functools import lru_cache
from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import EmptySearchResultError, FoundMoreThanOneError
from mex.common.fields import ALL_MODEL_CLASSES_BY_NAME
from mex.common.logging import logger
from mex.common.models import (
    MERGED_MODEL_CLASSES_BY_NAME,
    AnyMergedModel,
    MergedContactPoint,
    MergedModelTypeAdapter,
    MergedPerson,
)
from mex.common.types import AnyMergedIdentifier, MergedPersonIdentifier
from mex.common.utils import contains_any_types, group_fields_by_class_name
from mex.extractors.settings import Settings

MEX_EMAIL = "mex@rki.de"


PERSON_FIELDS_BY_CLASS_NAME = group_fields_by_class_name(
    ALL_MODEL_CLASSES_BY_NAME,
    lambda field_info: contains_any_types(field_info, MergedPersonIdentifier),
)


def ensure_list(values: object) -> list[object]:
    """Wrap single objects in lists, replace None with [] and return lists untouched."""
    if values is None:
        return []
    if isinstance(values, list):
        return values
    return [values]


@lru_cache(maxsize=1)
def get_mex_contact_point() -> MergedContactPoint:
    """Get the MEx ContactPoint from the backend."""
    connector = BackendApiConnector.get()
    response = connector.fetch_merged_items(
        MEX_EMAIL, ["MergedContactPoint"], None, 0, 1
    )
    if response.total == 0:
        msg = f"No ContactPoint for {MEX_EMAIL} found."
        raise EmptySearchResultError(msg)
    if response.total >= 1:
        msg = f"Found {response.total} ContactPoints for {MEX_EMAIL} found, expected 1."
        raise FoundMoreThanOneError(msg)
    return cast("MergedContactPoint", response.items[0])


def filter_person_references(merged_model: AnyMergedModel) -> AnyMergedModel:
    """Remove references to Person items.

    If field is required and empty after filtering, it is set to the mex@rki.de
        ContactPoint

    Args:
        merged_model: Merged model to check for person references.

    Returns:
        Merged Model without person references.
    """
    mex_contact_point = get_mex_contact_point()
    connector = BackendApiConnector.get()
    fields_allowing_persons = PERSON_FIELDS_BY_CLASS_NAME[merged_model.entityType]
    if not fields_allowing_persons:
        return merged_model
    for field in fields_allowing_persons:
        filtered_references: list[AnyMergedIdentifier] = []
        references = cast(
            "list[AnyMergedIdentifier]", ensure_list(getattr(merged_model, field))
        )
        for reference in references:
            response = connector.request(
                method="GET",
                endpoint=f"merged-item/{reference}",
            )
            referenced_model = MergedModelTypeAdapter.validate_python(response)
            if not isinstance(referenced_model, MergedPerson):
                filtered_references.append(reference)
        if not filtered_references and merged_model.model_fields[field].is_required():
            setattr(merged_model, field, mex_contact_point.identifier)
        else:
            setattr(merged_model, field, filtered_references)
    return merged_model


def get_merged_items() -> Generator[AnyMergedModel, None, None]:
    """Read merged items from backend."""
    connector = BackendApiConnector.get()
    settings = Settings.get()

    response = connector.fetch_merged_items(None, None, None, 0, 1)
    total_item_number = response.total

    item_number_limit = 100  # 100 is the maximum possible number per get-request

    logging_counter = 0

    allowlist_for_merged_items_to_extract = [
        item
        for item in MERGED_MODEL_CLASSES_BY_NAME
        if item not in settings.skip_merged_items
    ]

    for item_counter in range(0, total_item_number + 1, item_number_limit):
        response = connector.fetch_merged_items(
            None,
            allowlist_for_merged_items_to_extract,
            None,
            item_counter,
            item_number_limit,
        )
        for item in response.items:
            logging_counter += 1
            yield item
        logger.info(
            "%s of %s merged items were extracted.", logging_counter, total_item_number
        )

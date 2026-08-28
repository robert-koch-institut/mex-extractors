from typing import TYPE_CHECKING

from mex.common.backend_api.connector import BackendApiConnector, ReferenceFilter
from mex.common.exceptions import MExError

if TYPE_CHECKING:
    from mex.common.models import AnyMergedModel
    from mex.common.types import AnyMergedIdentifier


def get_publishable_merged_items(
    *,
    query_string: str | None = None,
    entity_type: list[str] | None = None,
    reference_filters: list[ReferenceFilter] | None = None,
) -> list[AnyMergedModel]:
    """Read publishable merged items from backend."""
    connector = BackendApiConnector.get()
    response = connector.fetch_all_publishable_merged_items(
        publishing_target="invenio",
        query_string=query_string,
        entity_type=entity_type,
        reference_filters=reference_filters,
    )
    return list(response)


def get_publishable_merged_item(
    identifier: AnyMergedIdentifier,
) -> AnyMergedModel:
    """Fetch a merged item from backend identified by its identifier.

    Args:
        identifier: Identifier of merged item of any entity Type

    Returns:
        the merged item of the identitifer

    Raises:
        MExError if not exactly one item is found.
    """
    connector = BackendApiConnector.get()

    result = connector.fetch_publishable_merged_items(
        publishing_target="invenio",
        identifier=identifier,
    ).items

    if len(result) == 0:
        msg = (
            f"Merged item '{identifier}' does not exist or is not publishable to the "
            f"publishing target."
        )
        raise MExError(msg)

    if len(result) > 1:
        msg = f"More than one merged item found for '{identifier}'."
        raise MExError(msg)

    return result[0]

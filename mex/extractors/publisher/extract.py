from typing import TYPE_CHECKING

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import MExError

if TYPE_CHECKING:
    from mex.common.models import AnyMergedModel


def get_publishable_merged_items(
    *,
    query_string: str | None = None,
    entity_type: list[str] | None = None,
    referenced_identifier: list[str] | None = None,
    reference_field: str | None = None,
) -> list[AnyMergedModel]:
    """Read publishable merged items from backend."""
    connector = BackendApiConnector.get()

    response = connector.fetch_all_publishable_merged_items(
        publishing_target="invenio",
        query_string=query_string,
        entity_type=entity_type,
        referenced_identifier=referenced_identifier,
        reference_field=reference_field,
    )
    return list(response)


def get_publishable_merged_item_by_identifier(
    identifier: str,
) -> AnyMergedModel:
    """Fetch a merged item from backend identified by its identifier."""
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

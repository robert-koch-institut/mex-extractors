from typing import TYPE_CHECKING

from mex.common.backend_api.connector import BackendApiConnector, ReferenceFilter

if TYPE_CHECKING:
    from mex.common.models import AnyMergedModel


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

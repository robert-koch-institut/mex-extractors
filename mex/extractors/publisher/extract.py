from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import AnyMergedModel


def get_publishable_merged_items(
    *,
    entity_type: list[str] | None = None,
    referenced_identifier: list[str] | None = None,
    reference_field: str | None = None,
) -> list[AnyMergedModel]:
    """Read publishable merged items from backend."""
    connector = BackendApiConnector.get()

    response = connector.fetch_all_merged_items(
        entity_type=entity_type,
        referenced_identifier=referenced_identifier,
        reference_field=reference_field,
    )
    return list(response)

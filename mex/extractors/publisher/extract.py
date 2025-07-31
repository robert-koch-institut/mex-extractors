from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import AnyMergedModel


def get_publishable_merged_items(
    *,
    entity_type: list[str] | None = None,
    primary_source_ids: list[str] | None = None,
) -> list[AnyMergedModel]:
    """Read publishable merged items from backend."""
    connector = BackendApiConnector.get()

    reference_field = "hadPrimarySource" if primary_source_ids else None

    response = connector.fetch_all_merged_items(
        entity_type=entity_type,
        referenced_identifier=primary_source_ids,
        reference_field=reference_field,
    )
    return list(response)

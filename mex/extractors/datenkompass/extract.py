from mex.common.backend_api.connector import BackendApiConnector
from mex.common.identity import get_provider
from mex.common.models import AnyMergedModel


def get_merged_items(
    query_string: str | None,
    entity_type: list[str],
    primary_source_ids: list[str] | None,
) -> list[AnyMergedModel]:
    """Read merged items from backend.

    Args:
        query_string: Query string.
        entity_type: List of entity types.
        primary_source_ids: List of primary source ids.

    Returns:
        List of merged items.
    """
    connector = BackendApiConnector.get()

    reference_field = "hadPrimarySource" if primary_source_ids else None

    response = connector.fetch_all_merged_items(
        query_string=query_string,
        entity_type=entity_type,
        referenced_identifier=primary_source_ids,
        reference_field=reference_field,
    )

    return list(response)


def get_relevant_primary_source_ids(relevant_primary_sources: list[str]) -> list[str]:
    """Get the IDs of the relevant primary sources.

    Args:
        relevant_primary_sources: List of primary sources.

    Returns:
        List of IDs of the relevant primary sources.
    """
    entity_type = ["MergedPrimarySource"]
    merged_primary_sources = list(get_merged_items(None, entity_type, None))
    provider = get_provider()

    return [
        str(mps.identifier)
        for mps in merged_primary_sources
        if mps.entityType == entity_type[0]
        and provider.fetch(stable_target_id=mps.identifier)[0].identifierInPrimarySource
        in relevant_primary_sources
    ]

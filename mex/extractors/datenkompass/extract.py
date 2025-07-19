from mex.common.backend_api.connector import BackendApiConnector
from mex.common.identity import get_provider
from mex.common.logging import logger
from mex.common.models import AnyMergedModel


def get_merged_items(
    query_string: str | None,
    entity_type: list[str],
    had_primary_source: list[str] | None,
) -> list[AnyMergedModel]:
    """Read merged items from backend.

    Args:
        query_string: Query string.
        entity_type: List of entity types.
        had_primary_source: List of primary sources.

    Returns:
        List of merged items.
    """
    connector = BackendApiConnector.get()

    response = connector.fetch_merged_items(
        query_string, entity_type, had_primary_source, 0, 1
    )
    total_item_number = 10  # response.total

    item_number_limit = 100  # 100 is the maximum possible number per get-request

    logging_counter = 0

    result: list[AnyMergedModel] = []
    for item_counter in range(0, total_item_number, item_number_limit):
        response = connector.fetch_merged_items(
            query_string,
            entity_type,
            had_primary_source,
            item_counter,
            item_number_limit,
        )
        logging_counter += len(response.items)
        result.extend(response.items)
    logger.debug(
        "%s of %s %ss were extracted.",
        logging_counter,
        total_item_number,
        entity_type,
    )
    return result


def get_relevant_primary_source_ids(relevant_primary_sources: list[str]) -> list[str]:
    """Get the IDs of the relevant primary sources.

    Args:
        relevant_primary_sources: List of primary sources.

    Returns:
        List of IDs of the relevant primary sources.
    """
    connector = BackendApiConnector.get()
    preview_primary_sources = connector.fetch_preview_items(
        query_string=None,
        entity_type=["MergedPrimarySource"],
        had_primary_source=None,
        skip=0,
        limit=100,  # change if Datenkompass still running when we have more than 100 PS
    ).items

    provider = get_provider()

    return [
        str(pps.identifier)
        for pps in preview_primary_sources
        if pps.entityType == "PreviewPrimarySource"
        and provider.fetch(stable_target_id=pps.identifier)[0].identifierInPrimarySource
        in relevant_primary_sources
    ]

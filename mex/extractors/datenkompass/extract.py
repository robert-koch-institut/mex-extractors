from mex.common.backend_api.connector import BackendApiConnector
from mex.common.identity import get_provider
from mex.common.logging import logger
from mex.common.models import AnyMergedModel


def get_merged_items(
    query_string: str | None,
    entity_type: list[str],
    had_primary_source: list[str] | None,
) -> list[AnyMergedModel]:
    """Read merged items from backend."""
    connector = BackendApiConnector.get()

    response = connector.fetch_merged_items(
        query_string, entity_type, had_primary_source, 0, 1
    )
    total_item_number = response.total

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
    """Get the IDs of the relevant primary sources."""
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

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.logging import logger
from mex.common.models import AnyMergedModel


def get_publishable_merged_items(
    *,
    entity_type: list[str] | None = None,
    had_primary_source: list[str] | None = None,
) -> list[AnyMergedModel]:
    """Read publishable merged items from backend."""
    items: list[AnyMergedModel] = []
    connector = BackendApiConnector.get()

    response = connector.fetch_merged_items(
        query_string=None,
        entity_type=entity_type,
        had_primary_source=had_primary_source,
        skip=0,
        limit=1,
    )
    total_item_number = response.total

    item_number_limit = 100  # 100 is the maximum possible number per get-request

    logging_counter = 0

    for item_counter in range(0, total_item_number + 1, item_number_limit):
        response = connector.fetch_merged_items(
            query_string=None,
            entity_type=entity_type,
            had_primary_source=had_primary_source,
            skip=item_counter,
            limit=item_number_limit,
        )
        logging_counter += len(response.items)
        items.extend(response.items)
        logger.info("collected %s of %s items", logging_counter, total_item_number)
    return items

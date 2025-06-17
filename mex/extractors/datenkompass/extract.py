from collections.abc import Generator

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.logging import logger
from mex.common.models import AnyMergedModel


def get_merged_items(
    entity_type: list[str], had_primary_source: list[str] | None
) -> Generator[AnyMergedModel, None, None]:
    """Read merged items from backend."""
    connector = BackendApiConnector.get()

    response = connector.fetch_merged_items(None, entity_type, had_primary_source, 0, 1)
    total_item_number = response.total

    item_number_limit = 100  # 100 is the maximum possible number per get-request

    logging_counter = 0

    for item_counter in range(0, total_item_number, item_number_limit):
        response = connector.fetch_merged_items(
            None,
            entity_type,
            had_primary_source,
            item_counter,
            item_number_limit,
        )
        for item in response.items:
            logging_counter += 1
            yield item
        logger.info(
            "%s of %s %ss were extracted.",
            logging_counter,
            total_item_number,
            entity_type,
        )

from collections.abc import Generator

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.logging import logger
from mex.common.models import AnyMergedModel


def get_merged_items() -> Generator[AnyMergedModel, None, None]:
    """Read merged items from backend."""
    connector = BackendApiConnector.get()

    response = connector.fetch_merged_items(None, None, 0, 1)
    total_item_number = response.total

    item_number_limit = 100  # 100 is the maximum possible number per get-request

    logging_counter = 0
    for item_counter in range(0, total_item_number, item_number_limit):
        response = connector.fetch_merged_items(
            None, None, item_counter, item_number_limit
        )
        for item in response.items:
            logging_counter += 1
            yield item
        logger.info(
            "%s of %s merged items where extracted.", logging_counter, total_item_number
        )

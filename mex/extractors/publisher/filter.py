from collections.abc import Generator, Iterable

from mex.common.logging import logger
from mex.common.models import AnyMergedModel
from mex.extractors.settings import Settings


def filter_merged_items(
    items: Iterable[AnyMergedModel],
) -> Generator[AnyMergedModel, None, None]:
    """Filter to be published items by allow list."""
    settings = Settings.get()

    skipped_items = 0
    total_items = 0

    for item in items:
        if item.entityType in settings.skip_merged_items:
            skipped_items += 1
            total_items += 1
        if item.entityType not in settings.skip_merged_items:
            total_items += 1
            yield item

    logger.info("%s of %s merged items where filtered out.", skipped_items, total_items)

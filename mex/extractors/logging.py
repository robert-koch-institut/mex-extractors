from collections.abc import Generator, Iterable, Sized
from typing import TypeVar

from mex.common.logging import logger
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.settings import Settings

T = TypeVar("T")


def log_filter(
    identifier_in_primary_source: str | None,
    primary_source_id: MergedPrimarySourceIdentifier,
    reason: str,
) -> None:
    """Log filtered sources.

    Args:
        identifier_in_primary_source: optional identifier in the primary source
        primary_source_id: identifier of the primary source
        reason: string explaining the reason for filtering
    """
    logger.info(
        "[source filtered] reason: %s, "
        "had_primary_source: %s, "
        "identifier_in_primary_source: %s",
        reason,
        primary_source_id,
        identifier_in_primary_source,
    )


def watch_progress[T](
    iterable: Iterable[T],
    description: str,
) -> Generator[T]:
    """Log the progress of an iterable at regular intervals.

    Args:
        iterable: The iterable to track
        description: Description for the operation

    Yields:
        Items from the original iterable
    """
    if isinstance(iterable, Sized):
        total = f"/{len(iterable)}"
    else:
        total = ""
    settings = Settings.get()
    for index, item in enumerate(iterable, start=1):
        if index % settings.log_frequency == 0:
            logger.info("%s: %s%s", description, index, total)
        yield item
    logger.info("%s: done", description)

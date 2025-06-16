from mex.common.logging import logger
from mex.common.types import Identifier


def log_filter(
    identifier_in_primary_source: str | None,
    primary_source_id: Identifier,
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

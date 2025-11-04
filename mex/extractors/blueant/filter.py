from collections.abc import Iterable

from mex.common.utils import contains_any
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.logging import log_filter, watch_progress
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings


def filter_and_log_blueant_sources(
    sources: Iterable[BlueAntSource],
) -> list[BlueAntSource]:
    """Filter Blueant sources and log filtered sources.

    Args:
        sources: BlueantSources

    Returns:
        List of Blue Ant sources
    """
    return [
        source
        for source in watch_progress(sources, "filter_and_log_blueant_sources")
        if filter_and_log_blueant_source(source)
    ]


def filter_and_log_blueant_source(
    source: BlueAntSource,
) -> bool:
    """Filter a BlueantSource according to settings and log filtering.

    Args:
        source: BlueantSource

    Settings:
        blueant.skip_labels: Skip source if these terms are in the label

    Returns:
        False if source is filtered out, else True
    """
    settings = Settings.get()
    identifier_in_primary_source = source.number
    if contains_any(source.name, settings.blueant.skip_labels):
        log_filter(
            identifier_in_primary_source,
            get_extracted_primary_source_id_by_name("blueant"),
            f"Name [{source.name}] in settings.blueant.skip_labels",
        )
        return False
    return True

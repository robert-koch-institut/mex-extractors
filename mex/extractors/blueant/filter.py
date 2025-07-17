from collections.abc import Generator, Iterable

from mex.common.logging import watch
from mex.common.types import MergedPrimarySourceIdentifier
from mex.common.utils import contains_any
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.logging import log_filter
from mex.extractors.settings import Settings


@watch()
def filter_and_log_blueant_sources(
    sources: Iterable[BlueAntSource],
    primary_source_id: MergedPrimarySourceIdentifier,
) -> Generator[BlueAntSource, None, None]:
    """Filter Blueant sources and log filtered sources.

    Args:
        sources: BlueantSources Generator
        primary_source_id: Identifier of primary source

    Returns:
        Generator for Blue Ant sources
    """
    for source in sources:
        if filter_and_log_blueant_source(source, primary_source_id):
            yield source


def filter_and_log_blueant_source(
    source: BlueAntSource,
    primary_source_id: MergedPrimarySourceIdentifier,
) -> bool:
    """Filter a BlueantSource according to settings and log filtering.

    Args:
        source: BlueantSource
        primary_source_id: Identifier of primary source

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
            primary_source_id,
            f"Name [{source.name}] in settings.blueant.skip_labels",
        )
        return False
    return True

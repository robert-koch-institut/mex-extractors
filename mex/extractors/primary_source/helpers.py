from functools import lru_cache
from typing import TYPE_CHECKING

from mex.common.primary_source.helpers import get_extracted_primary_source_by_name
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from mex.common.models import ExtractedPrimarySource
    from mex.common.types import MergedPrimarySourceIdentifier


@lru_cache(maxsize=3)
def cached_load_extracted_primary_source_by_name(
    name: str,
) -> ExtractedPrimarySource | None:
    """Use helper function to look up a primary source and to load and return it.

    A primary source is searched by its name and loaded into the configured sink
    and returned.

    Args:
        name: name of the primary source

    Returns:
        ExtractedPrimarySource if one matching primary source is found.
        None if multiple matches / no match is found
    """
    extracted_primary_source = get_extracted_primary_source_by_name(name)

    if extracted_primary_source is None:
        return None

    load([extracted_primary_source])

    return extracted_primary_source


def get_extracted_primary_source_id_by_name(
    name: str,
) -> MergedPrimarySourceIdentifier:
    """Use helper function to return the stableTargetId of a found primary source.

    Args:
        name: name of the primary source

    Returns:
        ExtractedPrimarySource stableTargetId if one matching primary source is found.
        raise error if multiple matches / no match is found
    """
    if extracted_primary_source := cached_load_extracted_primary_source_by_name(name):
        return extracted_primary_source.stableTargetId
    msg = f"Primary source name {name} not found."
    raise NameError(msg)

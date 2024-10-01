from functools import cache

from mex.common.primary_source.helpers import get_extracted_primary_source_by_name
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.sinks import load


@cache
def load_extracted_primary_source_and_return_stabletargetid(
    name: str,
) -> MergedPrimarySourceIdentifier | None:
    """Get extracted primary source and load it and return its Id.

    Args:
        name: name of the primary source to get

    Returns:
        ExtractedPrimarySource.stableTargetId if a primary source is
           found, else None.
    """
    extracted_primary_source = get_extracted_primary_source_by_name(name)
    if extracted_primary_source:
        load([extracted_primary_source])
        return extracted_primary_source.stableTargetId

    return None

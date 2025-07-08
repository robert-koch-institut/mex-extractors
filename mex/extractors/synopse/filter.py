from collections.abc import Generator

from mex.common.models import ExtractedPrimarySource
from mex.extractors.logging import log_filter
from mex.extractors.synopse.models.variable import SynopseVariable


def filter_and_log_synopse_variables(
    synopse_variables: Generator[SynopseVariable, None, None],
    extracted_primary_source: ExtractedPrimarySource,
) -> list[SynopseVariable]:
    """Filter out and log variables used for internal context.

    Args:
        synopse_variables: iterable of synopse variables
        extracted_primary_source: primary source for report server platform

    Returns:
        list of filtered synopse variables
    """
    filtered_variables: list[SynopseVariable] = []
    for variable in synopse_variables:
        if variable.int_var:
            log_filter(
                variable.synopse_id,
                extracted_primary_source.stableTargetId,
                "variable used for internal context",
            )
        else:
            filtered_variables.append(variable)
    return filtered_variables

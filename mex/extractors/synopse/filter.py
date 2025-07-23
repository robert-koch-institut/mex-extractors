from collections.abc import Generator

from mex.common.logging import logger
from mex.common.models import ExtractedPrimarySource
from mex.extractors.logging import log_filter
from mex.extractors.synopse.models.variable import SynopseVariable


def filter_and_log_synopse_variables(
    synopse_variables: Iterable[SynopseVariable],
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
    skipped_variable_ids: list[str] = []
    for variable in synopse_variables:
        if variable.int_var:
            skipped_variable_ids.append(variable.synopse_id)
        else:
            filtered_variables.append(variable)
    logger.info(
        "Skipped variable id sample: %s, "
        "had_primary_source: %s, "
        "amount of skipped variables: %s",
        skipped_variable_ids[:10],
        extracted_primary_source.stableTargetId,
        len(skipped_variable_ids),
    )
    return filtered_variables

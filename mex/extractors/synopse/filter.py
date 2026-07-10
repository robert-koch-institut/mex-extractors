from typing import TYPE_CHECKING

from mex.common.logging import logger
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.synopse.models.variable import Variablenuebersicht


def filter_and_log_synopse_variables(
    synopse_variables: list[Variablenuebersicht],
) -> list[Variablenuebersicht]:
    """Filter out and log variables used for internal context.

    Args:
        synopse_variables: list of synopse variables

    Returns:
        list of filtered synopse variables
    """
    filtered_variables: list[Variablenuebersicht] = []
    skipped_variable_ids: list[str] = []
    for variable in synopse_variables:
        if variable.IntVar:
            skipped_variable_ids.append(variable.SymopseID)
        else:
            filtered_variables.append(variable)
    logger.info(
        "Skipped variable id sample: %s, "
        "had_primary_source: %s, "
        "amount of skipped variables: %s",
        skipped_variable_ids[:10],
        get_extracted_primary_source_id_by_name("report-server"),
        len(skipped_variable_ids),
    )
    return filtered_variables

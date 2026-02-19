from typing import TYPE_CHECKING

from mex.extractors.logging import log_filter
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel


def filter_and_log_cc2_aux_model(
    extracted_cc2_aux_model: Iterable[Cc2AuxModel],
) -> list[Cc2AuxModel]:
    """Filter out and log cc2 aux model variables which are not in_database_static.

    Args:
        extracted_cc2_aux_model: cc2 aux model variables
        extracted_primary_source: primary source for report server platform

    Returns:
        List of filtered cc2 aux model variables
    """
    filtered = []
    for variable in extracted_cc2_aux_model:
        if variable.in_database_static:
            filtered.append(variable)
        else:
            log_filter(
                variable.variable_name,
                get_extracted_primary_source_id_by_name("nokeda"),
                "in_database_static is False for cc2_aux_model variable.",
            )
    return filtered

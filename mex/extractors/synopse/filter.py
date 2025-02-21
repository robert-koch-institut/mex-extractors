from collections.abc import Generator, Iterable

from mex.common.models import ExtractedPrimarySource
from mex.extractors.logging import log_filter
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.variable import SynopseVariable


def filter_and_log_access_platforms(
    synopse_studies: Iterable[SynopseStudy],
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[SynopseStudy, None, None]:
    """Filter out and log studies that cannot be accessed via an internal network drive.

    Args:
        synopse_studies: iterable of synopse studies
        extracted_primary_source: primary source for report server platform

    Returns:
        Generator for filtered synopse studies
    """
    for study in synopse_studies:
        if study.plattform_adresse not in [
            "interne Datennutzung",
            "noch nicht erstellt",
        ]:
            yield study
        else:
            log_filter(
                study.plattform_adresse,
                extracted_primary_source.stableTargetId,
                "Platform address cannot be accessed via an internal network drive.",
            )


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

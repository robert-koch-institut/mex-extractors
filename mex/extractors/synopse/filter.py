from collections.abc import Generator, Iterable

from mex.common.logging import logger
from mex.common.models import ExtractedPrimarySource
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.variable import SynopseVariable


def filter_and_log_access_platforms(
    synopse_studies: Iterable[SynopseStudy],
    extracted_primary_source: ExtractedPrimarySource,
) -> list[SynopseStudy]:
    """Filter out and log studies that cannot be accessed via an internal network drive.

    Args:
        synopse_studies: iterable of synopse studies
        extracted_primary_source: primary source for report server platform

    Returns:
        List of filtered synopse studies
    """
    filtered_studies: list[SynopseStudy] = []
    skipped_study_ids: list[str] = []
    for study in synopse_studies:
        if study.plattform_adresse in [
            "interne Datennutzung",
            "noch nicht erstellt",
        ]:
            skipped_study_ids.append(study.plattform_adresse)
        else:
            filtered_studies.append(study)
    logger.info(
        "Skipped studies id sample: %s, "
        "had_primary_source: %s, "
        "amount of skipped studies: %s",
        skipped_study_ids[:10],
        extracted_primary_source.stableTargetId,
        len(skipped_study_ids),
    )
    return filtered_studies


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

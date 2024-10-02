from collections.abc import Generator, Iterable

from mex.common.models import ExtractedPrimarySource
from mex.extractors.logging import log_filter
from mex.extractors.synopse.models.study import SynopseStudy


def filter_and_log_access_platforms(
    synopse_studies: Iterable[SynopseStudy],
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[SynopseStudy, None, None]:
    """Filter out and log studies which annot be accessed via an internal netword drive.

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
                "Platform adress cannot be accessed via an internal netword drive.",
            )

from mex.common.models import ExtractedPrimarySource
from mex.extractors.synopse.filter import filter_and_log_access_platforms
from mex.extractors.synopse.models.study import SynopseStudy


def test_filter_and_log_variables(
    synopse_studies: list[SynopseStudy],
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> None:
    assert len(list(synopse_studies)) == 6
    synopse_studies_filtered = filter_and_log_access_platforms(
        synopse_studies, extracted_primary_sources["report-server"]
    )
    assert len(list(synopse_studies_filtered)) == 4

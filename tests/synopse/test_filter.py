from mex.common.models import ExtractedPrimarySource
from mex.extractors.synopse.filter import (
    filter_and_log_access_platforms,
    filter_and_log_synopse_variables,
)
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.variable import SynopseVariable


def test_filter_and_log_access_plattforms(
    synopse_studies: list[SynopseStudy],
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> None:
    assert len(list(synopse_studies)) == 6
    synopse_studies_filtered = filter_and_log_access_platforms(
        synopse_studies, extracted_primary_sources["report-server"]
    )
    assert len(list(synopse_studies_filtered)) == 5


def test_filter_and_log_synopse_variables(
    synopse_variables: list[SynopseVariable],
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> None:
    assert len(list(synopse_variables)) == 6
    synopse_variables_filtered = filter_and_log_synopse_variables(
        synopse_variables, extracted_primary_sources["report-server"]
    )
    assert len(list(synopse_variables_filtered)) == 4

from mex.common.models import ExtractedPrimarySource
from mex.extractors.synopse.filter import (
    filter_and_log_synopse_variables,
)
from mex.extractors.synopse.models.variable import SynopseVariable


def test_filter_and_log_synopse_variables(
    synopse_variables: list[SynopseVariable],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    assert len(list(synopse_variables)) == 3
    synopse_variables_filtered = filter_and_log_synopse_variables(
        synopse_variables, extracted_primary_sources["report-server"]
    )
    assert len(list(synopse_variables_filtered)) == 1

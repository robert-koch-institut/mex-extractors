from itertools import tee

from mex.common.models import ExtractedPrimarySource
from mex.extractors.sumo.extract import extract_cc2_aux_model
from mex.extractors.sumo.filter import filter_and_log_cc2_aux_model


def test_filter_and_log_variables(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_data = extract_cc2_aux_model()
    extracted_data_gens = tee(extracted_data, 2)
    assert len(list(extracted_data_gens[0])) == 2
    extracted_data = filter_and_log_cc2_aux_model(
        extracted_data_gens[1], extracted_primary_sources["nokeda"]
    )
    assert len(list(extracted_data)) == 1

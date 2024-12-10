from itertools import tee

from mex.common.models import ExtractedPrimarySource
from mex.extractors.sumo.extract import extract_cc2_aux_model
from mex.extractors.sumo.filter import filter_and_log_cc2_aux_model


def test_filter_and_log_variables(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_models = extract_cc2_aux_model()
    extracted_model_gens = tee(extracted_models, 2)
    assert len(list(extracted_model_gens[0])) == 2
    extracted_models = filter_and_log_cc2_aux_model(
        extracted_model_gens[1], extracted_primary_sources["nokeda"]
    )
    assert len(list(extracted_models)) == 1

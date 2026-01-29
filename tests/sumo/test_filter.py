from mex.extractors.sumo.extract import extract_cc2_aux_model
from mex.extractors.sumo.filter import filter_and_log_cc2_aux_model


def test_filter_and_log_variables() -> None:
    extracted_models = extract_cc2_aux_model()
    assert len(extracted_models) == 2
    extracted_models = filter_and_log_cc2_aux_model(extracted_models)
    assert len(extracted_models) == 1

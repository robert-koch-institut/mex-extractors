from mex.extractors.odk.filter import is_invalid_odk_variable


def test_is_valid_odk_variable() -> None:
    assert is_invalid_odk_variable("end_group") is True
    assert is_invalid_odk_variable("begin_group") is False

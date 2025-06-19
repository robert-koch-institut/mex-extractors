import pytest

from mex.extractors.igs.extract import extract_igs_schemas


@pytest.mark.usefixtures("mocked_igs")
def test_extract_igs_schemas() -> None:
    schemas = extract_igs_schemas()
    assert schemas["enum_schema"].model_dump() == {"enum": ["enum"]}
    assert schemas["properties_schema"].model_dump() == {"properties": {"key": "value"}}

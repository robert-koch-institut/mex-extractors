import pytest

from mex.extractors.igs.connector import IGSConnector


@pytest.mark.integration
def test_datscha_connector_item_urls() -> None:
    igs_schemas = IGSConnector.get()
    raw_json = igs_schemas.get_json_from_api()

    assert "components" in raw_json
    assert "schemas" in raw_json["components"]
    assert len(raw_json["components"]["schemas"]) > 100

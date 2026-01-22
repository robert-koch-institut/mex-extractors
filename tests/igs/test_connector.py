import pytest

from mex.extractors.igs.connector import IGSConnector


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_igs_connector_json_from_api() -> None:
    igs_schemas = IGSConnector.get()
    raw_json = igs_schemas.get_json_from_api()

    assert "components" in raw_json
    assert "schemas" in raw_json["components"]
    assert len(raw_json["components"]["schemas"]) == 2


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_igs_connector_get_endpoint_count() -> None:
    connector = IGSConnector.get()
    count = connector.get_endpoint_count(
        endpoint="/samples/count", params={"pathogens": "LISP"}
    )
    assert count != "0"

import pytest

from mex.extractors.datscha_web.connector import DatschaWebConnector


@pytest.mark.requires_rki_infrastructure
def test_datscha_connector_item_urls() -> None:
    datscha = DatschaWebConnector.get()
    item_urls = datscha.get_item_urls()

    assert len(list(item_urls)) > 10


@pytest.mark.requires_rki_infrastructure
def test_datscha_connector_get_item() -> None:
    datscha = DatschaWebConnector.get()
    item_urls = datscha.get_item_urls()
    datscha_web_item = datscha.get_item(item_urls[0])

    assert datscha_web_item.model_dump().get("item_id")

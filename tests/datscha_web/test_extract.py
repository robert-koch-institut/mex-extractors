import pytest
from pytest import MonkeyPatch

import mex.common.wikidata.extract
from mex.extractors.datscha_web.extract import (
    extract_datscha_web_items,
    extract_datscha_web_organizations,
)
from mex.extractors.datscha_web.models.item import DatschaWebItem


@pytest.mark.usefixtures("mocked_datscha_web")
def test_extract_datscha_web_items_mocked() -> None:
    datscha_web_sources = list(extract_datscha_web_items())

    assert len(datscha_web_sources) == 2
    assert datscha_web_sources[0].item_id == 17
    assert (
        datscha_web_sources[0].auskunftsperson
        == "Coolname, Cordula/ Ausgedacht, Alwina"
    )
    assert datscha_web_sources[1].item_id == 92
    assert datscha_web_sources[1].auskunftsperson is None


def test_extract_datscha_web_organizations(
    datscha_web_item: DatschaWebItem, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mex.extractors.datscha_web.extract,
        "get_wikidata_extracted_organization_id_by_name",
        lambda x: "test",
    )
    result = extract_datscha_web_organizations([datscha_web_item])

    assert result == {
        datscha_web_item.empfaenger_der_daten_im_drittstaat: "test",
    }

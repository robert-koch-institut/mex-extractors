import json
from unittest.mock import MagicMock, Mock

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from mex.extractors.locations import extract
from mex.extractors.locations.extract import (
    extract_country_wikidata_ids_from_eu_vocabularies_json,
    extract_wikidata_locations,
)
from mex.extractors.settings import ExtractorsSettings


@pytest.fixture
def mocked_assets_connector(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the assets connector used by the extract module and return it."""
    connector = MagicMock()
    monkeypatch.setattr(extract, "get_assets_connector", lambda: connector)
    return connector


def test_extract_country_wikidata_ids_from_eu_vocabularies_json(
    mocked_assets_connector: MagicMock,
) -> None:
    mocked_assets_connector.read.return_value = json.dumps(
        [
            {"wikidataId": ["http://www.wikidata.org/entity/Q183"]},
            {"wikidataId": ["https://www.wikidata.org/wiki/Q40"]},
            {"wikidataId": ["Q142"], "unrelated": "ignored"},
        ]
    )

    wikidata_ids = extract_country_wikidata_ids_from_eu_vocabularies_json()

    assert wikidata_ids == ["Q183", "Q40", "Q142"]
    settings = ExtractorsSettings.get()
    mocked_assets_connector.read.assert_called_once_with(
        settings.locations.country_list_path
    )


@pytest.mark.parametrize(
    "raw_countries",
    [
        [{"wikidataId": []}],
        [{"wikidataId": ["Q183", "Q40"]}],
        [{"somethingElse": ["Q183"]}],
    ],
    ids=["no wikidata id", "multiple wikidata ids", "missing field"],
)
def test_extract_country_wikidata_ids_rejects_invalid_countries(
    mocked_assets_connector: MagicMock,
    raw_countries: list[dict[str, list[str]]],
) -> None:
    mocked_assets_connector.read.return_value = json.dumps(raw_countries)

    with pytest.raises(ValidationError):
        extract_country_wikidata_ids_from_eu_vocabularies_json()


def test_extract_wikidata_locations(monkeypatch: MonkeyPatch) -> None:
    first, second = Mock(name="location_1"), Mock(name="location_2")
    mocked_get_locations = Mock(return_value=iter([first, second]))
    monkeypatch.setattr(extract, "get_wikidata_locations", mocked_get_locations)

    locations = list(extract_wikidata_locations(["Q2", "Q1"]))

    assert locations == [first, second]
    mocked_get_locations.assert_called_once_with(["Q1", "Q2"])

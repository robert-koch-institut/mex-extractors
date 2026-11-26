from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from mex.common.wikidata.connector import WikidataAPIConnector
from mex.extractors.locations.connector import WikidataSPARQLConnector

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pytest import MonkeyPatch


MOCKED_LOCATION_IDS = {"Q183", "Q42", "Q954"}


def create_raw_wikidata_location(item_id: str) -> dict[str, Any]:
    """Create a minimal raw Wikidata location, as returned by `wbgetentities`."""
    return {
        "id": item_id,
        "labels": {
            "de": {"language": "de", "value": f"Ort {item_id}"},
            "en": {"language": "en", "value": f"Place {item_id}"},
        },
        "claims": {"P1566": [{"mainsnak": {"datavalue": {"value": "7654321"}}}]},
        "aliases": {},
    }


@pytest.fixture
def mocked_wikidata_locations(monkeypatch: MonkeyPatch) -> None:
    """Mock the Wikidata SPARQL and the Wikidata API connector for locations.

    The SPARQL connector returns dummy region ids. The Wikidata API connector
    answers batch requests for locations with a dummy location per requested id.
    Use this together with `mocked_wikidata`, which replaces the API connector's
    session but only mocks the single-item request, not the batch request.
    """

    def __init__(self: WikidataSPARQLConnector) -> None:
        self.session = MagicMock()
        self.url = "https://mock-wikidata-sparql"

    monkeypatch.setattr(WikidataSPARQLConnector, "__init__", __init__)
    monkeypatch.setattr(
        WikidataSPARQLConnector,
        "extract_wikidata_location_ids",
        lambda _: ["Q183", "Q42"],
    )

    def get_wikidata_item_details_by_ids(
        _self: WikidataAPIConnector, item_ids: Sequence[str]
    ) -> dict[str, dict[str, Any]]:
        return {item_id: create_raw_wikidata_location(item_id) for item_id in item_ids}

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_ids",
        get_wikidata_item_details_by_ids,
    )

    # answer single-item requests for locations, keep any earlier mock for the rest
    get_item_details_by_id = WikidataAPIConnector.get_wikidata_item_details_by_id

    def get_wikidata_item_details_by_id(
        self: WikidataAPIConnector, item_id: str
    ) -> dict[str, Any]:
        if item_id in MOCKED_LOCATION_IDS:
            return create_raw_wikidata_location(item_id)
        return get_item_details_by_id(self, item_id)

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_id",
        get_wikidata_item_details_by_id,
    )

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from mex.extractors.locations.connector import WikidataSPARQLConnector

if TYPE_CHECKING:
    from pytest import MonkeyPatch


@pytest.fixture
def mocked_wikidata_locations(monkeypatch: MonkeyPatch) -> None:
    """Mock the Wikidata SPARQL connector to return dummy region ids."""

    def __init__(self: WikidataSPARQLConnector) -> None:
        self.session = MagicMock()
        self.url = "https://mock-wikidata-sparql"

    monkeypatch.setattr(WikidataSPARQLConnector, "__init__", __init__)
    monkeypatch.setattr(
        WikidataSPARQLConnector,
        "extract_wikidata_location_ids",
        lambda _: ["Q183", "Q42"],
    )

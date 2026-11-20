from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
import requests

from mex.common.connector import HTTPConnector
from mex.extractors.locations.connector import (
    WikidataSPARQLConnector,
)

if TYPE_CHECKING:
    from pytest import MonkeyPatch


def create_sparql_response(*item_ids: str) -> dict[str, Any]:
    """Create a minimal Wikidata SPARQL JSON response for the given item ids."""
    return {
        "results": {
            "bindings": [
                {
                    "item": {
                        "type": "uri",
                        "value": f"http://www.wikidata.org/entity/{item_id}",
                    }
                }
                for item_id in item_ids
            ]
        }
    }


@pytest.fixture
def mocked_sparql_session(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the session of the HTTPConnector with a MagicMock and return that."""
    mocked_session = MagicMock(spec=requests.Session, name="mocked_sparql_session")
    mocked_session.headers = {}

    def set_mocked_session(self: HTTPConnector) -> None:
        self.session = mocked_session

    monkeypatch.setattr(HTTPConnector, "_set_session", set_mocked_session)
    return mocked_session


@pytest.mark.usefixtures("mocked_sparql_session")
def test_extract_wikidata_location_ids_deduplicates_and_sorts() -> None:
    responses = [
        create_sparql_response("Q2", "Q1"),
        create_sparql_response("Q1", "Q3"),
        create_sparql_response(),
        create_sparql_response("Q10"),
        create_sparql_response("Q2"),
    ]
    connector = WikidataSPARQLConnector.get()
    connector.request = MagicMock(side_effect=responses)  # type: ignore[method-assign]

    assert connector.extract_wikidata_location_ids() == ["Q1", "Q10", "Q2", "Q3"]

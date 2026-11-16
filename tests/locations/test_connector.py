from typing import Any
from unittest.mock import MagicMock, call

import pytest
import requests
from pytest import MonkeyPatch

from mex.common.connector.http import HTTPConnector
from mex.common.settings import BaseSettings
from mex.extractors.locations.connector import (
    REGION_TYPE_IDS,
    WikidataSPARQLConnector,
)
from mex.extractors.locations.query import build_wikidata_query
from mex.extractors.settings import ExtractorsSettings


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
def mocked_request(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the connector's init and request method and return the request mock."""
    monkeypatch.setattr(WikidataSPARQLConnector, "__init__", lambda _: None)
    mocked = MagicMock()
    monkeypatch.setattr(WikidataSPARQLConnector, "request", mocked)
    return mocked


def test_set_url() -> None:
    connector = WikidataSPARQLConnector.__new__(WikidataSPARQLConnector)
    connector._set_url()

    settings = ExtractorsSettings.get()
    assert connector.url == str(settings.locations.sparql_url)


def test_set_session_sets_user_agent_headers(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        HTTPConnector,
        "_set_session",
        lambda self: setattr(self, "session", requests.Session()),
    )
    connector = WikidataSPARQLConnector.__new__(WikidataSPARQLConnector)
    connector._set_session()

    user_agent = BaseSettings.get().mex_web_user_agent
    assert connector.session.headers["User-Agent"] == user_agent
    assert connector.session.headers["Api-User-Agent"] == user_agent


def test_extract_wikidata_location_ids_deduplicates_and_sorts(
    mocked_request: MagicMock,
) -> None:
    responses = [
        create_sparql_response("Q2", "Q1"),
        create_sparql_response("Q1", "Q3"),
        create_sparql_response(),
        create_sparql_response("Q10"),
        create_sparql_response("Q2"),
    ]
    assert len(responses) == len(REGION_TYPE_IDS)
    mocked_request.side_effect = responses

    connector = WikidataSPARQLConnector()
    location_ids = connector.extract_wikidata_location_ids()

    assert location_ids == ["Q1", "Q10", "Q2", "Q3"]


def test_extract_wikidata_location_ids_queries_every_region_type(
    mocked_request: MagicMock,
) -> None:
    mocked_request.return_value = create_sparql_response()

    connector = WikidataSPARQLConnector()
    connector.extract_wikidata_location_ids()

    assert mocked_request.call_args_list == [
        call(
            "GET",
            params={
                "query": build_wikidata_query(region_type_id),
                "format": "json",
            },
            timeout=60,
        )
        for region_type_id in REGION_TYPE_IDS
    ]


def test_extract_wikidata_location_ids_without_results(
    mocked_request: MagicMock,
) -> None:
    mocked_request.return_value = create_sparql_response()

    connector = WikidataSPARQLConnector()

    assert connector.extract_wikidata_location_ids() == []

import json
import re
from itertools import islice
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests.models import Response

from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.extract import (
    fetch_all_vvt_pages_ids,
    get_page_data_by_id,
)
from tests.confluence_vvt.conftest import TEST_DATA_DIR


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_fetch_all_vvt_pages_ids() -> None:
    page_ids = list(fetch_all_vvt_pages_ids())
    assert all(re.match(r"\d+", id_) for id_ in page_ids)


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_get_page_data_by_id() -> None:
    page_ids = set(islice(fetch_all_vvt_pages_ids(), 5))

    response = get_page_data_by_id(page_ids)
    assert page_ids > {str(r.id) for r in response}


def test_fetch_all_data_page_ids_mocked(
    monkeypatch: MonkeyPatch,
) -> None:
    # first response with mocked test data
    response1 = Mock(spec=Response, status_code=200)
    with (TEST_DATA_DIR / "fetch_all_data_page_ids.json").open() as fh:
        response1.json.return_value = json.load(fh)

    # second response with empty data
    response2 = Mock(spec=Response, status_code=200)
    response2.json.return_value = {"results": []}

    # mocking connector session
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response1, response2])

    monkeypatch.setattr(
        ConfluenceVvtConnector,
        "__init__",
        lambda self: setattr(self, "session", session),
    )

    page_ids = list(fetch_all_vvt_pages_ids())

    expected = ["0101010101", "0202020202", "0303030303", "0404040404"]

    assert page_ids == expected


def test_fetch_all_pages_data_mocked(
    monkeypatch: MonkeyPatch, detail_page_data_json: dict[str, Any]
) -> None:
    response = Mock(spec=Response, status_code=200)
    response.json.return_value = detail_page_data_json
    page_label = Mock(spec=Response, status_code=200)
    page_label.json.return_value = {"results": [{"name": "vvt"}]}

    # mocking create_session function
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=(content for content in [page_label, response]))

    monkeypatch.setattr(
        ConfluenceVvtConnector,
        "__init__",
        lambda self: setattr(self, "session", session),
    )

    all_pages_data = list(get_page_data_by_id(["123457"]))

    assert len(all_pages_data) == 1
    all_pages_data_dict = all_pages_data[0].model_dump(exclude_none=True)
    assert all_pages_data_dict["id"] == 123457
    assert all_pages_data_dict["title"] == "Test Title"
    assert len(all_pages_data_dict["tables"]) == 2

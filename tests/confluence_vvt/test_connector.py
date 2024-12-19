from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests import HTTPError

from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.extract import fetch_all_vvt_pages_ids
from mex.extractors.confluence_vvt.models import (
    ConfluenceVvtHeading,
    ConfluenceVvtValue,
)


@pytest.fixture
def mocked_confluence_vvt(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the ConfluenceVvt session with a MagicMock session and return that."""
    mocked_session = MagicMock(spec=requests.Session)
    mocked_session.request = MagicMock(
        return_value=Mock(spec=requests.Response, status_code=200)
    )

    mocked_session.headers = {}

    def set_mocked_session(self: ConfluenceVvtConnector) -> None:
        self.session = mocked_session

    monkeypatch.setattr(ConfluenceVvtConnector, "_set_session", set_mocked_session)
    return mocked_session


@pytest.mark.integration
def test_initialization() -> None:
    connector = ConfluenceVvtConnector.get()
    try:
        connector._check_availability()
    except HTTPError:
        pytest.fail("HTTPError : Connector initialization failed.")


def test_initialization_mocked_auth_fail(
    mocked_confluence_vvt: requests.Session,
) -> None:
    error_message = {
        "statusCode": 401,
        "data": {
            "authorized": False,
            "valid": True,
            "allowedInReadOnlyMode": True,
            "errors": [],
            "successful": False,
        },
        "message": "No parent or not permitted to view content with id : ContentId{id=007}",
        "reason": "Not Found",
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 401
    mocked_response.json = MagicMock(return_value=error_message)
    mocked_confluence_vvt.request = MagicMock(return_value=mocked_response)

    connector = ConfluenceVvtConnector.get()

    response = connector.request("GET", "localhost")

    assert response["statusCode"] == 401


def test_initialization_mocked_server_error(
    mocked_confluence_vvt: requests.Session,
) -> None:
    error_message = {
        "statusCode": 500,
        "message": "Internal Server Error",
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 500
    mocked_response.json = MagicMock(return_value=error_message)
    mocked_confluence_vvt.request = MagicMock(return_value=mocked_response)

    connector = ConfluenceVvtConnector.get()

    response = connector.request("GET", "localhost")

    assert response["statusCode"] == 500


@pytest.mark.integration
def test_get_page_by_id() -> None:
    connector = ConfluenceVvtConnector.get()
    page_data = connector.get_page_by_id("89780861")
    assert len(page_data.model_dump()) == 2

    # extract
    result = {}
    for table in page_data.tables:
        complete_row_with_values = []
        for i, row in enumerate(table.rows):
            for k, cell in enumerate(row.cells):
                cells_text = {}
                if isinstance(cell, ConfluenceVvtHeading):
                    cells_text["heading"] = cell.text
                    complete_row_with_values.append(cells_text)
                    continue
                if isinstance(cell, ConfluenceVvtValue):
                    breakpoint()
                    complete_row_with_values[k] = {
                        **complete_row_with_values[k],
                        "values": cell.texts,
                    }
                    # cells_text["values"] = cell.texts
                    # complete_row_with_values[k] = complete_row_with_values[k].update(
                    #     {"values": cell.texts}
                    # )
                    # complete_row_with_values.append(cells_text)
                    continue
            result[f"heading_value_pair_row_{i}"] = complete_row_with_values
            if (i + 1) % 2 == 0:
                complete_row_with_values.clear()
    breakpoint()

    # start by mapping rows to values below. do it row loop here.

    all_pages_ids = fetch_all_vvt_pages_ids()
    # for page_id in all_pages_ids:
    #     pagedata = connector.get_page_by_id(page_id)


nested_dict = {
    "heading_value_pair_row_1": [
        {"heading": "Interne nummer", "value": "DS-1234"},
        {"heading": "heading cell1", "value": "Bear. Wolf"},
    ],
    "heading_value_pair_row_2": [
        {"heading": "second row heading cell 1", "value": "second row value cell 1"},
        {"heading": "second row heading cell 2", "value": "second row value cell 2"},
        {"heading": "second row heading cell 3", "value": "second row value cell 3"},
    ],
    "heading_value_pair_row_3": [
        {
            "heading": "second row heading only cell",
            "value": "second row value only cell",
        },
    ],
}

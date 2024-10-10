from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests import HTTPError

from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.extract import fetch_all_data_page_ids


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
    all_pages_ids = fetch_all_data_page_ids()
    for page_id in all_pages_ids:
        connector.get_page_by_id(page_id)

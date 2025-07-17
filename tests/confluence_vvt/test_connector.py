from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests import HTTPError
from requests.models import Response

from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector


@pytest.fixture
def mocked_confluence_vvt_session(monkeypatch: MonkeyPatch) -> MagicMock:
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
    mocked_confluence_vvt_session: requests.Session,
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
    mocked_confluence_vvt_session.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = ConfluenceVvtConnector.get()

    response = connector.request("GET", "localhost")

    assert response["statusCode"] == 401


def test_initialization_mocked_server_error(
    mocked_confluence_vvt_session: requests.Session,
) -> None:
    error_message = {
        "statusCode": 500,
        "message": "Internal Server Error",
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 500
    mocked_response.json = MagicMock(return_value=error_message)
    mocked_confluence_vvt_session.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = ConfluenceVvtConnector.get()

    response = connector.request("GET", "localhost")

    assert response["statusCode"] == 500


@pytest.mark.integration
def test_get_page_by_id(
    monkeypatch: MonkeyPatch, detail_page_data_json: dict[str, Any]
) -> None:
    connector = ConfluenceVvtConnector.get()
    response = Mock(spec=Response, status_code=200)
    response.json.return_value = detail_page_data_json
    page_label = Mock(spec=Response, status_code=200)
    page_label.json.return_value = {"results": [{"name": "vvt"}]}

    connector.session.get = MagicMock(  # type: ignore[method-assign]
        side_effect=(content for content in [page_label, response])
    )

    monkeypatch.setattr(
        ConfluenceVvtConnector,
        "__init__",
        lambda self: setattr(self, "session", connector.session),
    )
    page = connector.get_page_by_id("123457")

    assert page is not None
    page_dict = page.model_dump(exclude_none=True)
    assert page_dict["id"] == 123457
    assert page_dict["title"] == "Test Title"
    assert len(page_dict["tables"]) == 2

from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch

from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataVersionFiles,
)
from tests.open_data.mocked_open_data import (
    create_mocked_file_response,
    create_mocked_parent_response,
)


@pytest.fixture
def mocked_open_data_connector(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the OpenDataConnector with a MagicMock session and return that."""
    mocked_session = MagicMock(spec=requests.Session, name="mocked_opendata_session")

    mocked_session.headers = {}

    def set_mocked_session(self: OpenDataConnector) -> None:
        self.session = mocked_session

    monkeypatch.setattr(OpenDataConnector, "_set_session", set_mocked_session)
    return mocked_session


def test_get_parent_resources(mocked_open_data_connector: OpenDataConnector) -> None:
    dummy_parents = create_mocked_parent_response()
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_parents)
    mocked_open_data_connector.request = MagicMock(return_value=mocked_response)

    connector = OpenDataConnector.get()
    results = connector.get_parent_resources()

    assert (
        mocked_open_data_connector.request.call_count == 4
    )  # 1x connector initializing, 1x getting total, 2x for 2 pages
    assert results == 2 * [
        OpenDataParentResource.model_validate(dummy_parents["hits"]["hits"][0]),
        OpenDataParentResource.model_validate(dummy_parents["hits"]["hits"][1]),
    ]


def test_get_files_for_resource_version(
    mocked_open_data_connector: OpenDataConnector,
) -> None:
    # get mock responses
    dummy_versions = create_mocked_file_response()
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_versions)
    mocked_open_data_connector.request = MagicMock(return_value=mocked_response)

    connector = OpenDataConnector.get()
    results = connector.get_files_for_resource_version(1001)

    assert (
        mocked_open_data_connector.request.call_count == 2
    )  # 1x connector initializing, 1x retrieving entries (no pagination here)
    assert results == [
        OpenDataVersionFiles.model_validate(dummy_versions["entries"][0]),
    ]

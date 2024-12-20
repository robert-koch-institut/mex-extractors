from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch

from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)


@pytest.fixture
def mocked_open_data(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the OpenDataConnector with a MagicMock session and return that."""
    mocked_session = MagicMock(spec=requests.Session, name="mocked_opendata_session")

    mocked_session.headers = {}

    def set_mocked_session(self: OpenDataConnector) -> None:
        self.session = mocked_session

    monkeypatch.setattr(OpenDataConnector, "_set_session", set_mocked_session)
    return mocked_session


def test_get_parent_resources(mocked_open_data: OpenDataConnector) -> None:
    # Create mock responses
    dummy_parents = {
        "hits": {
            "hits": [
                {"id": 1, "conceptrecid": "one"},
                {"id": 2, "conceptrecid": "two"},
                {"id": 3, "conceptrecid": "three"},
            ],
            "total": 200,
        }
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_parents)
    mocked_open_data.request = MagicMock(return_value=mocked_response)

    connector = OpenDataConnector.get()
    results = list(connector.get_parent_resources())

    assert (
        mocked_open_data.request.call_count == 4
    )  # 1x connector initializing, 1x getting total, 2x for 2 pages
    assert results == [
        OpenDataParentResource(
            conceptrecid="one", id=1, modified=None, conceptdoi=None, metadata=None
        ),
        OpenDataParentResource(
            conceptrecid="two", id=2, modified=None, conceptdoi=None, metadata=None
        ),
        OpenDataParentResource(
            conceptrecid="three", id=3, modified=None, conceptdoi=None, metadata=None
        ),
        OpenDataParentResource(
            conceptrecid="one", id=1, modified=None, conceptdoi=None, metadata=None
        ),
        OpenDataParentResource(
            conceptrecid="two", id=2, modified=None, conceptdoi=None, metadata=None
        ),
        OpenDataParentResource(
            conceptrecid="three", id=3, modified=None, conceptdoi=None, metadata=None
        ),
    ]


def test_get_resource_versions(mocked_open_data: OpenDataConnector) -> None:
    # Create mock responses
    dummy_versions = {
        "hits": {
            "hits": [
                {"id": 1, "conceptrecid": "one"},
                {"id": 2, "conceptrecid": "one"},
                {"id": 3, "conceptrecid": "one"},
            ],
            "total": 201,
        }
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_versions)
    mocked_open_data.request = MagicMock(return_value=mocked_response)

    connector = OpenDataConnector.get()
    results = list(connector.get_resource_versions(1))

    assert (
        mocked_open_data.request.call_count == 5
    )  # 1x connector initializing, 1x getting total, 3x for 3 pages
    assert results == [
        OpenDataResourceVersion(
            conceptrecid="one",
            id=1,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=2,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=3,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=1,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=2,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=3,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=1,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=2,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
        OpenDataResourceVersion(
            conceptrecid="one",
            id=3,
            created=None,
            doi_url=None,
            metadata=None,
            files=[],
            modified=None,
        ),
    ]


def test_get_oldest_resource_version(mocked_open_data: OpenDataConnector) -> None:
    # Create mock responses
    dummy_versions = {
        "hits": {
            "hits": [
                {"id": 1, "conceptrecid": "one"},
                {"id": 2, "conceptrecid": "one"},
                {"id": 3, "conceptrecid": "one"},
            ],
            "total": 999,
        }
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_versions)
    mocked_open_data.request = MagicMock(return_value=mocked_response)

    connector = OpenDataConnector.get()
    results = connector.get_oldest_resource_version(3)

    assert (
        mocked_open_data.request.call_count == 2
    )  # 1x connector initializing, 1x for 1st page
    assert results == OpenDataResourceVersion(
        conceptrecid="one",
        id=1,
        created=None,
        doi_url=None,
        metadata=None,
        files=[],
        modified=None,
    )

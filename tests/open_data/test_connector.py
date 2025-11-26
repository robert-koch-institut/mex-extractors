from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests import HTTPError

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
    mocked_open_data_connector.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = OpenDataConnector.get()
    results = connector.get_parent_resources()

    assert (
        mocked_open_data_connector.request.call_count == 4
    )  # 1x connector initializing, 1x getting total, 2x for 2 pages (1-25, 26-42)
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
    mocked_open_data_connector.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = OpenDataConnector.get()
    results = connector.get_files_for_resource_version(1001)

    assert (
        mocked_open_data_connector.request.call_count == 2
    )  # 1x connector initializing, 1x retrieving entries (no pagination here)
    assert results == [
        OpenDataVersionFiles.model_validate(dummy_versions["entries"][0]),
    ]


def test_get_schema_zipfile_first_spelling(
    mocked_open_data_connector: OpenDataConnector,
) -> None:
    # Mock a successful response
    mock_success_response = Mock(spec=requests.Response)
    mock_success_response.status_code = 200

    mocked_open_data_connector.request = MagicMock(return_value=mock_success_response)  # type: ignore[method-assign]
    connector = OpenDataConnector.get()
    results = connector.get_schema_zipfile(1001)

    assert (
        mocked_open_data_connector.request.call_count == 2
    )  # 1x connector initializing, 1x retrieving entries

    called_url = mocked_open_data_connector.request.call_args_list[1].args[1]

    assert "1001" in called_url
    assert "Metadaten.zip" in called_url
    assert results is mock_success_response


def test_get_schema_zipfile_second_spelling(
    mocked_open_data_connector: OpenDataConnector,
) -> None:
    # Mock a successful response
    mock_success_response = Mock(spec=requests.Response)
    mock_success_response.status_code = 200

    # Mock a failed response
    mock_failure_response = Mock(spec=requests.Response)
    mock_failure_response.status_code = 404

    mocked_open_data_connector.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            mock_success_response,  # connector initializing succeeds
            mock_failure_response,  # first spelling fails
            mock_success_response,  # second spelling succeeds
        ]
    )

    connector = OpenDataConnector.get()
    results = connector.get_schema_zipfile(1001)

    assert (
        mocked_open_data_connector.request.call_count == 3
    )  # 1x connector initializing, 1x fail (1st spelling), 1x success (2nd spelling)

    first_called_url = mocked_open_data_connector.request.call_args_list[1].args[1]
    second_called_url = mocked_open_data_connector.request.call_args_list[2].args[1]

    assert "1001" in first_called_url
    assert "Metadaten.zip" in first_called_url
    assert "1001" in second_called_url
    assert "Metadata.zip" in second_called_url
    assert results is mock_success_response


def test_get_schema_zipfile_raises_if_no_zip_found(
    mocked_open_data_connector: OpenDataConnector,
) -> None:
    # Mock a successful response
    mock_success_response = Mock(spec=requests.Response)
    mock_success_response.status_code = 200

    # Mock a failed response
    mock_failure_response = Mock(spec=requests.Response)
    mock_failure_response.status_code = 404

    mocked_open_data_connector.request = Mock(  # type: ignore[method-assign]
        side_effect=[
            mock_success_response,  # connector initializing succeeds
            mock_failure_response,  # first spelling fails
            mock_failure_response,  # second spelling fails, too
        ]
    )

    connector = OpenDataConnector.get()

    with pytest.raises(HTTPError) as excinfo:
        connector.get_schema_zipfile(1001)

    msg = str(excinfo.value)
    assert "No metadata zip file found for record version 1001" in msg

from unittest.mock import MagicMock, Mock

import pytest
from pytest import MonkeyPatch
from requests.exceptions import HTTPError

from mex.extractors.drop import DropApiConnector


@pytest.mark.skip("Mex-drop might not be running. To be fixed in MX-1576")
@pytest.mark.requires_rki_infrastructure
def test_list_files() -> None:
    connector = DropApiConnector.get()
    files = connector.list_files("test")
    assert files == ["default"]


@pytest.mark.skip("Mex-drop might not be running. To be fixed in MX-1576")
@pytest.mark.requires_rki_infrastructure
def test_list_files_of_non_existing_x_system() -> None:
    connector = DropApiConnector.get()
    with pytest.raises(
        HTTPError, match=r"404.*The requested x-system was not found on this server"
    ):
        connector.list_files("does-not-exist")


def test_list_files_mocked(monkeypatch: MonkeyPatch) -> None:
    mocked_send_request = MagicMock(
        spec=DropApiConnector._send_request,
        return_value=Mock(json=MagicMock(return_value={"entity-types": ["default"]})),
    )
    monkeypatch.setattr(DropApiConnector, "_send_request", mocked_send_request)
    connector = DropApiConnector.get()
    files = connector.list_files("test")
    assert files == ["default"]


@pytest.mark.skip("Mex-drop might not be running. To be fixed in MX-1576")
@pytest.mark.requires_rki_infrastructure
def test_get_file() -> None:
    connector = DropApiConnector.get()
    content = connector.get_file("test", "default")
    assert content == {"foo": "bar", "list": [1, 2, "foo"], "nested": {"foo": "bar"}}


@pytest.mark.skip("Mex-drop might not be running. To be fixed in MX-1576")
@pytest.mark.requires_rki_infrastructure
def test_get_file_that_does_not_exist() -> None:
    connector = DropApiConnector.get()
    with pytest.raises(
        HTTPError, match=r"404 .*The requested data was not found on this server"
    ):
        connector.get_file("test", "does-not-exist")


def test_get_file_mocked(monkeypatch: MonkeyPatch) -> None:
    mocked_send_request = MagicMock(
        spec=DropApiConnector._send_request,
        return_value=Mock(json=MagicMock(return_value={"foo": "bar"})),
    )
    monkeypatch.setattr(DropApiConnector, "_send_request", mocked_send_request)
    connector = DropApiConnector.get()
    content = connector.get_file("test", "default")
    assert content == {"foo": "bar"}

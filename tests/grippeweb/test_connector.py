from typing import TYPE_CHECKING, cast
from unittest.mock import ANY, MagicMock

import pyodbc
import pytest

from mex.extractors.grippeweb.connector import GrippewebConnector
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from pytest import MonkeyPatch


@pytest.fixture(autouse=True)
def _clear_connector_singleton() -> None:
    """Ensure a fresh connector instance for each test."""
    GrippewebConnector.__instances__.clear()  # type: ignore[attr-defined]


def test_parse_rows_success(monkeypatch: MonkeyPatch) -> None:
    mocked_connection = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (
            "AAA",
            "2023-11-01 00:00:00.0000000",
            "2023-12-01 00:00:00.0000000",
            "1",
        ),
        (
            "BBB",
            "2023-12-01 00:00:00.0000000",
            "2024-01-01 00:00:00.0000000",
            "2",
        ),
    ]
    cursor.description = [
        ["Id"],
        ["StartedOn"],
        ["FinishedOn"],
        ["RepeatAfterDays"],
    ]
    mocked_connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(
        GrippewebConnector, "_setup_connection", lambda self: mocked_connection
    )

    connection = GrippewebConnector.get()
    columns = connection.parse_columns_by_column_name("vActualQuestion")

    assert columns == {
        "Id": ["AAA", "BBB"],
        "StartedOn": ["2023-11-01 00:00:00.0000000", "2023-12-01 00:00:00.0000000"],
        "FinishedOn": ["2023-12-01 00:00:00.0000000", "2024-01-01 00:00:00.0000000"],
        "RepeatAfterDays": ["1", "2"],
    }


def test_setup_connection_non_windows(monkeypatch: MonkeyPatch) -> None:
    """Test connection setup triggers kinit on non-Windows platforms."""
    monkeypatch.setattr("platform.system", lambda: "Linux")

    mock_process = MagicMock()
    mock_process.communicate.return_value = ("kinit success", "")
    mock_popen = MagicMock(return_value=mock_process)
    monkeypatch.setattr("mex.extractors.grippeweb.connector.Popen", mock_popen)

    mock_pyodbc_connect = MagicMock()
    monkeypatch.setattr("pyodbc.connect", mock_pyodbc_connect)

    # Initialize connector
    connector = GrippewebConnector.get()
    settings = Settings.get()

    # Assert kinit subprocess was called
    mock_popen.assert_called_once_with(
        ["kinit", settings.kerberos_user, "-V"],
        stdout=ANY,
        stdin=ANY,
        stderr=ANY,
        encoding="utf-8",
    )
    mock_process.communicate.assert_called_once_with(
        input=settings.kerberos_password.get_secret_value()
    )
    # Assert pyodbc connection was established
    mock_pyodbc_connect.assert_called_once_with(settings.grippeweb.mssql_connection_dsn)


def test_setup_connection_windows(monkeypatch: MonkeyPatch) -> None:
    """Test connection setup skips kinit on Windows platforms."""
    monkeypatch.setattr("platform.system", lambda: "Windows")

    mock_popen = MagicMock()
    monkeypatch.setattr("mex.extractors.grippeweb.connector.Popen", mock_popen)

    mock_pyodbc_connect = MagicMock()
    monkeypatch.setattr("pyodbc.connect", mock_pyodbc_connect)

    GrippewebConnector.get()

    mock_popen.assert_not_called()
    mock_pyodbc_connect.assert_called_once()


def test_parse_rows_retry_on_pyodbc_error(monkeypatch: MonkeyPatch) -> None:
    """Test that a pyodbc.Error triggers a reconnect and retry."""
    # Prevent actual connection setup
    mock_connection = MagicMock()
    mock_setup = MagicMock(return_value=mock_connection)
    monkeypatch.setattr(GrippewebConnector, "_setup_connection", mock_setup)

    mock_reconnect = MagicMock()
    monkeypatch.setattr(GrippewebConnector, "reconnect", mock_reconnect)

    connector = GrippewebConnector.get()

    # Setup cursor to fail on first call, succeed on second call
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = [pyodbc.Error("Kerberos Ticket Expired"), None]
    mock_cursor.fetchall.return_value = [("AAA",)]
    mock_cursor.description = [["Id"]]

    # Cast connector._connection.cursor to MagicMock to appease the type checker
    cast(
        "MagicMock", connector._connection.cursor
    ).return_value.__enter__.return_value = mock_cursor

    # Call the method
    result = connector.parse_columns_by_column_name("vActualQuestion")

    # Assertions
    assert result == {"Id": ["AAA"]}
    assert mock_cursor.execute.call_count == 2  # It tried twice
    mock_reconnect.assert_called_once()  # It reconnected in between


def test_close_suppresses_error(monkeypatch: MonkeyPatch) -> None:
    """Test that closing a dead connection safely catches pyodbc.Error."""
    mock_setup = MagicMock()
    monkeypatch.setattr(GrippewebConnector, "_setup_connection", mock_setup)

    connector = GrippewebConnector.get()
    connector._connection = MagicMock()

    # Cast connector._connection.close to MagicMock to appease the type checker
    cast("MagicMock", connector._connection.close).side_effect = pyodbc.Error(
        "Connection already dead"
    )

    # This should not raise an exception because of contextlib.suppress
    connector.close()

    cast("MagicMock", connector._connection.close).assert_called_once()

from typing import TYPE_CHECKING, cast
from unittest.mock import ANY, MagicMock

import pyodbc  # type: ignore[import-not-found]

from mex.extractors.grippeweb.connector import GrippewebConnector
from mex.extractors.settings import ExtractorSettings

if TYPE_CHECKING:
    from pytest import MonkeyPatch


def test_parse_rows_success() -> None:
    """Test the happy path of parsing rows by column name."""
    connector = object.__new__(GrippewebConnector)

    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("AAA", "2023-11-01 00:00:00.0000000", "2023-12-01 00:00:00.0000000", "1"),
        ("BBB", "2023-12-01 00:00:00.0000000", "2024-01-01 00:00:00.0000000", "2"),
    ]
    cursor.description = [["Id"], ["StartedOn"], ["FinishedOn"], ["RepeatAfterDays"]]

    connector._connection = MagicMock()
    cast(
        "MagicMock", connector._connection.cursor
    ).return_value.__enter__.return_value = cursor

    columns = connector.parse_columns_by_column_name("vActualQuestion")

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

    settings = ExtractorSettings.get()

    connector = object.__new__(GrippewebConnector)
    # Call __init__ from the class to satisfy mypy's strict instance checking
    GrippewebConnector.__init__(connector)

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
    mock_pyodbc_connect.assert_called_once_with(settings.grippeweb.mssql_connection_dsn)


def test_setup_connection_windows(monkeypatch: MonkeyPatch) -> None:
    """Test connection setup skips kinit on Windows platforms."""
    monkeypatch.setattr("platform.system", lambda: "Windows")

    mock_popen = MagicMock()
    monkeypatch.setattr("mex.extractors.grippeweb.connector.Popen", mock_popen)

    mock_pyodbc_connect = MagicMock()
    monkeypatch.setattr("pyodbc.connect", mock_pyodbc_connect)

    connector = object.__new__(GrippewebConnector)
    # Call __init__ from the class to satisfy mypy's strict instance checking
    GrippewebConnector.__init__(connector)

    mock_popen.assert_not_called()
    mock_pyodbc_connect.assert_called_once()


def test_parse_rows_retry_on_pyodbc_error(monkeypatch: MonkeyPatch) -> None:
    """Test that a pyodbc.Error triggers a reconnect and retry."""
    mock_reconnect = MagicMock()
    monkeypatch.setattr(GrippewebConnector, "reconnect", mock_reconnect)

    connector = object.__new__(GrippewebConnector)
    connector._connection = MagicMock()

    # Setup cursor to fail on first call, succeed on second call
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = [pyodbc.Error("Kerberos Ticket Expired"), None]
    mock_cursor.fetchall.return_value = [("AAA",)]
    mock_cursor.description = [["Id"]]

    cast(
        "MagicMock", connector._connection.cursor
    ).return_value.__enter__.return_value = mock_cursor

    result = connector.parse_columns_by_column_name("vActualQuestion")

    assert result == {"Id": ["AAA"]}
    assert mock_cursor.execute.call_count == 2
    mock_reconnect.assert_called_once()


def test_close_suppresses_error() -> None:
    """Test that closing a dead connection safely catches pyodbc.Error."""
    connector = object.__new__(GrippewebConnector)
    connector._connection = MagicMock()

    cast("MagicMock", connector._connection.close).side_effect = pyodbc.Error(
        "Connection dead"
    )

    connector.close()

    cast("MagicMock", connector._connection.close).assert_called_once()

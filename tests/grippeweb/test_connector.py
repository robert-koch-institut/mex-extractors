from unittest.mock import MagicMock

from pytest import MonkeyPatch

from mex.extractors.grippeweb.connector import GrippewebConnector


def test_parse_rows(monkeypatch: MonkeyPatch) -> None:
    def mocked_init(self: GrippewebConnector) -> None:
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
        self._connection = MagicMock()
        self._connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(GrippewebConnector, "__init__", mocked_init)
    connection = GrippewebConnector.get()

    columns = connection.parse_columns_by_column_name("vActualQuestion")

    assert columns == {
        "Id": ["AAA", "BBB"],
        "StartedOn": ["2023-11-01 00:00:00.0000000", "2023-12-01 00:00:00.0000000"],
        "FinishedOn": ["2023-12-01 00:00:00.0000000", "2024-01-01 00:00:00.0000000"],
        "RepeatAfterDays": ["1", "2"],
    }

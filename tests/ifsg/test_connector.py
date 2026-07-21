import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from mex.extractors.ifsg.connector import IFSGConnector
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type

if TYPE_CHECKING:
    from pydantic import BaseModel
    from pytest import MonkeyPatch


def test_parse_rows(monkeypatch: MonkeyPatch) -> None:
    def mocked_init(self: IFSGConnector) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            (
                1,
                0,
                datetime.datetime(2015, 1, 12, 8, 24, 23, 413000),  # noqa: DTZ001
                b"\x00\x00\x00\x00\x00\x0b\xf6p",
            ),
            (
                1,
                11,
                datetime.datetime(2015, 1, 12, 8, 24, 23, 413000),  # noqa: DTZ001
                b"\x00\x00\x00\x00\x00\x0b\xf6q",
            ),
        ]
        cursor.description = [["IdSchema"], ["IdType"], ["UpdatedAt"], ["ts"]]
        self._connection = MagicMock()
        self._connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(IFSGConnector, "__init__", mocked_init)
    connection = IFSGConnector.get()

    rows = connection.parse_rows(MetaSchema2Type)

    assert rows == [
        {
            "IdSchema": 1,
            "IdType": 0,
            "UpdatedAt": datetime.datetime(2015, 1, 12, 8, 24, 23, 413000),  # noqa: DTZ001
            "ts": b"\x00\x00\x00\x00\x00\x0b\xf6p",
        },
        {
            "IdSchema": 1,
            "IdType": 11,
            "UpdatedAt": datetime.datetime(2015, 1, 12, 8, 24, 23, 413000),  # noqa: DTZ001
            "ts": b"\x00\x00\x00\x00\x00\x0b\xf6q",
        },
    ]


@pytest.mark.integration
def test_parse_rows_integration(
    mocked_ifsg_sql_tables: dict[type[BaseModel], list[dict[str, Any]]],
) -> None:
    """Parse every IFSG table against the seeded SQL Server fixture (sql-testing-seed).

    Requires a live, seeded server reachable via the ifsg DSN
    (``MEX_EXTRACTORS_IFSG__MSSQL_CONNECTION_DSN``); kerberos stays disabled so the
    connector uses the DSN's SQL auth directly. Rows are validated into their models
    so the assertion is robust to column-name and row-order differences.
    """
    connection = IFSGConnector()
    try:
        for model, expected_dicts in mocked_ifsg_sql_tables.items():
            actual = [model.model_validate(row) for row in connection.parse_rows(model)]
            expected = [model.model_validate(item) for item in expected_dicts]
            assert sorted(actual, key=str) == sorted(expected, key=str)
    finally:
        connection.close()

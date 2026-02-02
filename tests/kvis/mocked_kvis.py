from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.kvis.connector import KVISConnector
from mex.extractors.kvis.models.table_models import (
    KVISFieldValues,
    KVISVariables,
)


@pytest.fixture
def mocked_kvis(
    # mocked_kvis_sql_tables: dict[type[BaseModel], list[dict[str, Any]]],
    mocked_kvisfieldvalues: list[KVISFieldValues],
    mocked_kvisvariables: list[KVISVariables],
    monkeypatch: MonkeyPatch,
) -> None:
    """Mock KVIS connector."""
    mocked_kvis_sql_tables = {
        KVISFieldValues: [
            KVISFieldValues.model_dump(item) for item in mocked_kvisfieldvalues
        ],
        KVISVariables: [
            KVISVariables.model_dump(item) for item in mocked_kvisvariables
        ],
    }

    def mocked_init(self: KVISConnector) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = ["mocked"]
        self._connection = MagicMock()
        self._connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(KVISConnector, "__init__", mocked_init)

    monkeypatch.setattr(
        KVISConnector,
        "parse_rows",
        lambda self, model: mocked_kvis_sql_tables[model],
    )

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


@pytest.fixture
def mocked_kvisfieldvalues() -> list[KVISFieldValues]:
    return [
        KVISFieldValues(
            field_value_list_name="STRING",
            field_value="one",
            field_value_long_text="the number one",
        ),
        KVISFieldValues(
            field_value_list_name="STRING",
            field_value="two",
            field_value_long_text="the number two",
        ),
        KVISFieldValues(
            field_value_list_name="STRING",
            field_value="three",
            field_value_long_text="the number three",
        ),
        KVISFieldValues(
            field_value_list_name="BOOL",
            field_value="0",
            field_value_long_text="it is false",
        ),
        KVISFieldValues(
            field_value_list_name="BOOL",
            field_value="1",
            field_value_long_text="it is true",
        ),
    ]


@pytest.fixture
def mocked_kvisvariables() -> list[KVISVariables]:
    return [
        KVISVariables(
            file_type="file with integers",
            datatype_description="integer field",
            field_description="some integer field",
            field_name_short="int",
            field_name_long="Integer",
            fvlist_name=None,
        ),
        KVISVariables(
            file_type="file with strings and bools",
            datatype_description="string field",
            field_description="some text field",
            field_name_short="str",
            field_name_long="string",
            fvlist_name="STRING",
        ),
        KVISVariables(
            file_type="file with strings and bools",
            datatype_description="bool field",
            field_description="a boolean field for flagging",
            field_name_short="bool",
            field_name_long="boolean",
            fvlist_name="BOOL",
        ),
    ]

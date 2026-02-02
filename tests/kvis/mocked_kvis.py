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
            field_value="1",
            field_value_long_text="one",
        ),
        KVISFieldValues(
            field_value_list_name="BOOL",
            field_value="1",
            field_value_long_text="it is true",
        ),
        KVISFieldValues(
            field_value_list_name="STRING",
            field_value="2",
            field_value_long_text="two",
        ),
        KVISFieldValues(
            field_value_list_name="STRING",
            field_value="3",
            field_value_long_text="three",
        ),
        KVISFieldValues(
            field_value_list_name="BOOL",
            field_value="0",
            field_value_long_text="it is false",
        ),
    ]


@pytest.fixture
def mocked_kvisvariables() -> list[KVISVariables]:
    return [
        KVISVariables(
            file_type="file type 1",
            datatype_description="integer",
            field_description="field description",
            field_name_short="field name short",
            field_name_long="field name long",
            fvlist_name=None,
        ),
        KVISVariables(
            file_type="another file type",
            datatype_description="string field",
            field_description="some text field",
            field_name_short="str",
            field_name_long="string",
            fvlist_name="STRING",
        ),
        KVISVariables(
            file_type="another file type",
            datatype_description="bool",
            field_description="a boolean field for flagging",
            field_name_short="bit",
            field_name_long="bool",
            fvlist_name="BOOL",
        ),
    ]

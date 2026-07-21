import pytest

from mex.extractors.kvis.connector import KVISConnector
from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables


@pytest.mark.usefixtures("mocked_kvis")
def test_parse_rows() -> None:
    connection = KVISConnector.get()
    rows = connection.parse_rows(KVISFieldValues)
    assert rows == [
        {
            "field_value_list_name": "STRING",
            "field_value": "one",
            "field_value_long_text": "the number one",
        },
        {
            "field_value_list_name": "STRING",
            "field_value": "two",
            "field_value_long_text": "the number two",
        },
        {
            "field_value_list_name": "STRING",
            "field_value": "three",
            "field_value_long_text": "the number three",
        },
        {
            "field_value_list_name": "BOOL",
            "field_value": "0",
            "field_value_long_text": "it is false",
        },
        {
            "field_value_list_name": "BOOL",
            "field_value": "1",
            "field_value_long_text": "it is true",
        },
    ]


@pytest.mark.integration
def test_parse_rows_integration(
    mocked_kvisfieldvalues: list[KVISFieldValues],
    mocked_kvisvariables: list[KVISVariables],
) -> None:
    """Parse both KVIS tables against the seeded SQL Server fixture (sql-testing-seed).

    Requires a live, seeded server reachable via the kvis DSN
    (``MEX_EXTRACTORS_KVIS__MSSQL_CONNECTION_DSN``); kerberos stays disabled so the
    connector uses the DSN's SQL auth directly. Rows are validated into their models
    so the assertion is robust to column-name and row-order differences.
    """
    connection = KVISConnector()
    try:
        field_values = [
            KVISFieldValues.model_validate(row)
            for row in connection.parse_rows(KVISFieldValues)
        ]
        variables = [
            KVISVariables.model_validate(row)
            for row in connection.parse_rows(KVISVariables)
        ]
    finally:
        connection.close()

    assert sorted(field_values, key=str) == sorted(mocked_kvisfieldvalues, key=str)
    assert sorted(variables, key=str) == sorted(mocked_kvisvariables, key=str)

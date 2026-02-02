import pytest

from mex.extractors.kvis.connector import KVISConnector
from mex.extractors.kvis.models.table_models import KVISFieldValues


@pytest.mark.usefixtures("mocked_kvis")
def test_parse_rows() -> None:
    connection = KVISConnector.get()
    rows = connection.parse_rows(KVISFieldValues)
    assert rows == [
        {
            "field_value_list_name":"STRING",
            "field_value":"1",
            "field_value_long_text":"one",
        },
        {
            "field_value_list_name":"STRING",
            "field_value":"2",
            "field_value_long_text":"two",
        },
        {
            "field_value_list_name":"STRING",
            "field_value":"3",
            "field_value_long_text":"three",
        },
        {
            "field_value_list_name":"BOOL",
            "field_value":"1",
            "field_value_long_text":"it is true",
        },
        {
            "field_value_list_name":"BOOL",
            "field_value":"0",
            "field_value_long_text":"it is false",
        },
    ]

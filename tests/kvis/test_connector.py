import pytest

from mex.extractors.kvis.connector import KVISConnector
from mex.extractors.kvis.models.table_models import KVISFieldValuesTable


@pytest.mark.usefixtures("mocked_kvis")
def test_parse_rows() -> None:
    connection = KVISConnector.get()
    rows = connection.parse_rows(KVISFieldValuesTable)
    assert rows == [
        {
            "field_value_list_name": "field value list name",
            "field_value": "field value",
            "field_value_long_text": "field value long text",
        },
        {
            "field_value_list_name": "another list name",
            "field_value": "more values",
            "field_value_long_text": "and now also some longer text with more words",
        },
    ]

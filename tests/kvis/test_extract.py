import pytest

from mex.extractors.kvis.extract import extract_sql_table
from mex.extractors.kvis.models.table_models import KVISVariablesTable


@pytest.mark.usefixtures("mocked_kvis")
def test_extract_sql_table() -> None:
    result = extract_sql_table(KVISVariablesTable)
    expected = [
        KVISVariablesTable(
            file_type="file type",
            datatype_description="datatype description",
            field_description="field description",
            field_name_short="field name short",
            field_name_long="field name long",
            fvlist_name="fvlist name",
        ),
        KVISVariablesTable(
            file_type="some more file types",
            datatype_description="some more datatype descriptions",
            field_description="some more field descriptions",
            field_name_short="some more field name shorts",
            field_name_long="some more field name longs",
            fvlist_name="some more fvlist names",
        ),
    ]
    assert result == expected

import pytest

from mex.extractors.kvis.extract import extract_sql_table
from mex.extractors.kvis.models.table_models import KVISVariables


@pytest.mark.usefixtures("mocked_kvis")
def test_extract_sql_table() -> None:
    result = extract_sql_table(KVISVariables)
    expected = [
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
    assert result == expected

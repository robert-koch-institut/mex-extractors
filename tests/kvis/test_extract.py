import pytest

from mex.extractors.kvis.extract import extract_sql_table
from mex.extractors.kvis.models.table_models import KVISVariables


@pytest.mark.usefixtures("mocked_kvis")
def test_extract_sql_table() -> None:
    result = extract_sql_table(KVISVariables)
    expected = [
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
    assert result == expected

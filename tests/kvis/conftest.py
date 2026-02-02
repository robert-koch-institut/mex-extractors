import pytest

from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables


@pytest.fixture
def mocked_kvisfieldvalues() -> list[KVISFieldValues]:
    return [
        KVISFieldValues(
            field_value_list_name="field value list name",
            field_value="field value",
            field_value_long_text="field value long text",
        ),
        KVISFieldValues(
            field_value_list_name="another list name",
            field_value="more values",
            field_value_long_text="and now also some longer text with more words",
        ),
    ]


@pytest.fixture
def mocked_kvisvariables() -> list[KVISVariables]:
    return [
        KVISVariables(
            file_type="file type",
            datatype_description="datatype description",
            field_description="field description",
            field_name_short="field name short",
            field_name_long="field name long",
            fvlist_name="fvlist name",
        ),
        KVISVariables(
            file_type="some more file types",
            datatype_description="some more datatype descriptions",
            field_description="some more field descriptions",
            field_name_short="some more field name shorts",
            field_name_long="some more field name longs",
            fvlist_name="some more fvlist names",
        ),
    ]

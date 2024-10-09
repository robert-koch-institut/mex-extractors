from mex.extractors.ifsg.filter import (
    filter_id_type_of_diseases,
    filter_id_type_with_max_id_schema,
    filter_variables,
    get_max_id_schema,
)
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType


def test_get_max_id_schema(meta_schema2type: list[MetaSchema2Type]) -> None:
    max_id_schema = get_max_id_schema(meta_schema2type)

    assert max_id_schema == 42


def test_filter_id_type_with_max_id_schema(
    meta_schema2type: list[MetaSchema2Type],
) -> None:
    id_type_with_max_id_schema = filter_id_type_with_max_id_schema(meta_schema2type, 42)
    expected = [11]

    assert id_type_with_max_id_schema == expected


def test_filter_id_type_of_diseases(meta_type: list[MetaType]) -> None:
    id_types = filter_id_type_of_diseases([1, 2, 101, 102], meta_type)

    assert id_types == [101, 102]


def test_filter_variables(
    meta_field: list[MetaField],
    meta_schema2field: list[MetaSchema2Field],
) -> None:
    variables = filter_variables(meta_field, meta_schema2field, 10, [101, 102])

    expected = {
        "gui_text": "Id der Version",
        "gui_tool_tip": "lokaler",
        "id_catalogue": 0,
        "id_type": 101,
        "id_data_type": 0,
        "id_field": 1,
        "id_field_type": 3,
        "to_transport": 0,
        "sort": 1,
        "statement_area_group": "Epi",
    }

    assert variables[0].model_dump(exclude_defaults=True) == expected

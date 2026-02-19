from typing import TYPE_CHECKING

from mex.extractors.ifsg.filter import (
    filter_id_type_of_diseases,
    filter_variables,
)

if TYPE_CHECKING:
    from mex.extractors.ifsg.models.meta_field import MetaField
    from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
    from mex.extractors.ifsg.models.meta_type import MetaType


def test_filter_id_type_of_diseases(
    meta_schema2type: list[MetaSchema2Type], meta_type: list[MetaType]
) -> None:
    id_types = filter_id_type_of_diseases(meta_schema2type, meta_type)
    assert id_types == [101, 102]


def test_filter_variables(
    meta_field: list[MetaField],
) -> None:
    variables = filter_variables(meta_field, [101, 102])

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

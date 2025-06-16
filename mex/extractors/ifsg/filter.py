import re

from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType


def filter_id_type_of_diseases(
    meta_schema2types: list[MetaSchema2Type], meta_type: list[MetaType]
) -> list[int]:
    """Filter id_types that correspond to a disease.

    Args:
        meta_schema2types: MetaSchema2Type list
        meta_type: MetaType list

    Returns:
        id_type list related to diseases
    """
    id_types_of_diseases: list[int] = []
    meta_type_row_by_id_type = {m.id_type: m for m in meta_type}
    for meta_schema2type in meta_schema2types:
        meta_type_row = meta_type_row_by_id_type.get(meta_schema2type.id_type)
        if meta_type_row and re.match(
            "(Disease71|Disease73)([A-Z]){3}", meta_type_row.sql_table_name
        ):
            id_types_of_diseases.append(meta_schema2type.id_type)
    return id_types_of_diseases


def filter_empty_statement_area_group(meta_field: list[MetaField]) -> list[MetaField]:
    """Filter out meta_field rows with emtpy statement area group.

    Args:
        meta_field: list of MetaField entities

    Return:
        filtered list of MetaField entities for variableGroup transformation
    """
    return [row for row in meta_field if row.statement_area_group]


def filter_variables(
    meta_field: list[MetaField],
    id_types_of_diseases: list[int],
) -> list[MetaField]:
    """Filter meta_field list for variable transformation.

    Args:
        meta_field: list of MetaField entities
        id_types_of_diseases: id_types related to diseases

    Return:
        filtered list of MetaField entities to transform into variables
    """
    filtered_variables = [
        row for row in meta_field if row.id_type in id_types_of_diseases
    ]
    return [
        row
        for row in filtered_variables
        if row.to_transport != 0 or row.id_field_type in {3, 4}
    ]

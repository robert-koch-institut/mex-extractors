import re

from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType


def get_max_id_schema(meta_schema2type: list[MetaSchema2Type]) -> int:
    """Return the latest id_schema.

    Args:
      meta_schema2type: MetaSchema2Type list

    Returns:
      latest id_schema
    """
    return max([m.id_schema for m in meta_schema2type])


def filter_id_type_with_max_id_schema(
    meta_schema2type: list[MetaSchema2Type], max_id_schema: int
) -> list[int]:
    """Filter for id_types with max id_schema.

    Args:
        meta_schema2type: MetaSchema2Type list
        max_id_schema: latest id_schema

    Returns:
      id_type list filtered for latest id_schema
    """
    return [m.id_type for m in meta_schema2type if m.id_schema == max_id_schema]


def filter_id_type_of_diseases(
    id_types_with_max_id_schema: list[int], meta_type: list[MetaType]
) -> list[int]:
    """Filter id_types that correspond to a disease.

    Args:
        id_types_with_max_id_schema: id_types with latest id_schema
        meta_type: MetaType list

    Returns:
        id_type list related to diseases
    """
    id_types_of_diseases: list[int] = []
    meta_type_row_by_id_type = {m.id_type: m for m in meta_type}
    for id_type in id_types_with_max_id_schema:
        meta_type_row = meta_type_row_by_id_type.get(id_type)
        if meta_type_row and re.match(
            "(Disease71|Disease73)([A-Z]){3}", meta_type_row.sql_table_name
        ):
            id_types_of_diseases.append(id_type)
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
    meta_schema2field: list[MetaSchema2Field],
    max_id_schema: int,
    id_types_of_diseases: list[int],
) -> list[MetaField]:
    """Filter meta_field list for variable transformation.

    Args:
        meta_field: list of MetaField entities
        meta_schema2field: MetaSchema2Field list
        max_id_schema: lates id_schema,
        id_types_of_diseases: id_types related to diseases

    Return:
        filtered list of MetaField entities to transform into variables
    """
    id_field_with_latest_schema = [
        row.id_field for row in meta_schema2field if row.id_schema == max_id_schema
    ]
    filtered_variables = [
        row for row in meta_field if row.id_field in id_field_with_latest_schema
    ]
    filtered_variables = [
        row for row in filtered_variables if row.id_type in id_types_of_diseases
    ]
    return [
        row
        for row in filtered_variables
        if row.to_transport != 0 or row.id_field_type == 3 or row.id_field_type == 4
    ]

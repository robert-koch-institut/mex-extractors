from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.ifsg.extract import extract_sql_table
from mex.extractors.ifsg.filter import (
    filter_empty_statement_area_group,
    filter_id_type_of_diseases,
    filter_variables,
)
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_datatype import MetaDataType
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType
from mex.extractors.ifsg.transform import (
    transform_ifsg_data_to_mex_variable_group,
    transform_ifsg_data_to_mex_variables,
    transform_resource_disease_to_mex_resource,
    transform_resource_parent_to_mex_resource,
    transform_resource_state_to_mex_resource,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="ifsg")
def ifsg_meta_catalogue2item() -> list[MetaCatalogue2Item]:
    """Extract `Catalogue2Item` table."""
    return extract_sql_table(MetaCatalogue2Item)


@asset(group_name="ifsg")
def ifsg_meta_datatype() -> list[MetaDataType]:
    """Extract `MetaDataType` table."""
    return extract_sql_table(MetaDataType)


@asset(group_name="ifsg")
def ifsg_meta_catalogue2item2schema() -> list[MetaCatalogue2Item2Schema]:
    """Extract `Catalogue2Item2Schema` table."""
    return extract_sql_table(MetaCatalogue2Item2Schema)


@asset(group_name="ifsg")
def ifsg_meta_disease() -> list[MetaDisease]:
    """Extract `Disease` table."""
    return extract_sql_table(MetaDisease)


@asset(group_name="ifsg")
def ifsg_meta_field() -> list[MetaField]:
    """Extract `Field` table."""
    return extract_sql_table(MetaField)


@asset(group_name="ifsg")
def ifsg_filtered_meta_fields(ifsg_meta_field: list[MetaField]) -> list[MetaField]:
    """Filter Metafield list for empty statement_area_group."""
    return filter_empty_statement_area_group(ifsg_meta_field)


@asset(group_name="ifsg")
def ifsg_filtered_variables(
    ifsg_meta_field: list[MetaField],
    id_types_of_diseases: list[int],
) -> list[MetaField]:
    """Filter MetaField list."""
    return filter_variables(ifsg_meta_field, id_types_of_diseases)


@asset(group_name="ifsg")
def ifsg_meta_item() -> list[MetaItem]:
    """Extract `Item` table."""
    return extract_sql_table(MetaItem)


@asset(group_name="ifsg")
def ifsg_meta_schema2field() -> list[MetaSchema2Field]:
    """Extract `Schema2Field` table."""
    return extract_sql_table(MetaSchema2Field)


@asset(group_name="ifsg")
def ifsg_meta_schema2type() -> list[MetaSchema2Type]:
    """Extract `Schema2Type` table."""
    return extract_sql_table(MetaSchema2Type)


@asset(group_name="ifsg")
def id_types_of_diseases(
    ifsg_meta_schema2type: list[MetaSchema2Type], ifsg_meta_type: list[MetaType]
) -> list[int]:
    """Extract id_types that correspond to a disease."""
    return filter_id_type_of_diseases(ifsg_meta_schema2type, ifsg_meta_type)


@asset(group_name="ifsg")
def ifsg_meta_type() -> list[MetaType]:
    """Extract `Type` table."""
    return extract_sql_table(MetaType)


@asset(group_name="ifsg")
def ifsg_resource_disease_dict() -> dict[str, Any]:
    """Extract `resource_disease` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "resource_disease.yaml")


@asset(group_name="ifsg")
def ifsg_resource_parent_dict() -> dict[str, Any]:
    """Extract `resource_parent` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "resource_parent.yaml")


@asset(group_name="ifsg")
def ifsg_resource_state_dict() -> dict[str, Any]:
    """Extract `resource_state` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "resource_state.yaml")


@asset(group_name="ifsg")
def ifsg_variable_group() -> dict[str, Any]:
    """Extract `ifsg_variable_group` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "variable-group.yaml")


@asset(group_name="ifsg")
def ifsg_extracted_resource_parent(
    ifsg_resource_parent_dict: dict[str, Any],
    unit_stable_target_ids_by_synonym: dict[
        str, list[MergedOrganizationalUnitIdentifier]
        ],
) -> ExtractedResource:
    """Extracted and loaded ifsg resource parent."""
    mex_resource_parent = transform_resource_parent_to_mex_resource(
        ResourceMapping.model_validate(ifsg_resource_parent_dict),
        unit_stable_target_ids_by_synonym,
    )

    load([mex_resource_parent])

    return mex_resource_parent


@asset(group_name="ifsg")
def ifsg_extracted_resources_state(
    ifsg_resource_state_dict: dict[str, Any],
    ifsg_extracted_resource_parent: ExtractedResource,
    unit_stable_target_ids_by_synonym: dict[
        str, list[MergedOrganizationalUnitIdentifier]
        ],
) -> list[ExtractedResource]:
    """Extracted and loaded ifsg resource disease."""
    mex_resource_state = transform_resource_state_to_mex_resource(
        ResourceMapping.model_validate(ifsg_resource_state_dict),
        ifsg_extracted_resource_parent,
        unit_stable_target_ids_by_synonym,
    )
    load(mex_resource_state)

    return mex_resource_state


@asset(group_name="ifsg")
def ifsg_extracted_resources_disease(  # noqa: PLR0913
    ifsg_resource_disease_dict: dict[str, Any],
    ifsg_extracted_resource_parent: ExtractedResource,
    ifsg_extracted_resources_state: list[ExtractedResource],
    ifsg_meta_disease: list[MetaDisease],
    ifsg_meta_type: list[MetaType],
    id_types_of_diseases: list[int],
    unit_stable_target_ids_by_synonym: dict[
        str, list[MergedOrganizationalUnitIdentifier]
        ],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Extracted and loaded ifsg resource disease."""
    mex_resource_disease = transform_resource_disease_to_mex_resource(
        ResourceMapping.model_validate(ifsg_resource_disease_dict),
        ifsg_extracted_resource_parent,
        ifsg_extracted_resources_state,
        ifsg_meta_disease,
        ifsg_meta_type,
        id_types_of_diseases,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
    )
    load(mex_resource_disease)

    return mex_resource_disease


@asset(group_name="ifsg")
def ifsg_extracted_variable_groups(
    ifsg_variable_group: dict[str, Any],
    ifsg_extracted_resources_disease: list[ExtractedResource],
    ifsg_filtered_meta_fields: list[MetaField],
    id_types_of_diseases: list[int],
) -> list[ExtractedVariableGroup]:
    """Extracted and loaded ifsg variable group."""
    extracted_variable_group = transform_ifsg_data_to_mex_variable_group(
        VariableGroupMapping.model_validate(ifsg_variable_group),
        ifsg_extracted_resources_disease,
        ifsg_filtered_meta_fields,
        id_types_of_diseases,
    )
    load(extracted_variable_group)

    return extracted_variable_group


@asset(group_name="ifsg")
def ifsg_extracted_variables(  # noqa: PLR0913
    ifsg_filtered_variables: list[MetaField],
    ifsg_extracted_resources_disease: list[ExtractedResource],
    ifsg_extracted_variable_groups: list[ExtractedVariableGroup],
    ifsg_meta_catalogue2item: list[MetaCatalogue2Item],
    ifsg_meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    ifsg_meta_item: list[MetaItem],
    ifsg_meta_datatype: list[MetaDataType],
    ifsg_meta_schema2field: list[MetaSchema2Field],
) -> list[ExtractedVariable]:
    """Extracted and loaded ifsg variable."""
    extracted_variables = transform_ifsg_data_to_mex_variables(
        ifsg_filtered_variables,
        ifsg_extracted_resources_disease,
        ifsg_extracted_variable_groups,
        ifsg_meta_catalogue2item,
        ifsg_meta_catalogue2item2schema,
        ifsg_meta_item,
        ifsg_meta_datatype,
        ifsg_meta_schema2field,
    )
    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the IFSG extractor job in-process."""
    run_job_in_process("ifsg")

from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
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


@asset(group_name="ifsg", deps=["extracted_primary_source_mex"])
def extracted_primary_sources_ifsg(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return ifsg primary source."""
    (extracted_primary_sources_ifsg,) = get_primary_sources_by_name(
        extracted_primary_sources, "ifsg"
    )
    load([extracted_primary_sources_ifsg])

    return extracted_primary_sources_ifsg


@asset(group_name="ifsg")
def meta_catalogue2item() -> list[MetaCatalogue2Item]:
    """Extract `Catalogue2Item` table."""
    return extract_sql_table(MetaCatalogue2Item)


@asset(group_name="ifsg")
def meta_datatype() -> list[MetaDataType]:
    """Extract `MetaDataType` table."""
    return extract_sql_table(MetaDataType)


@asset(group_name="ifsg")
def meta_catalogue2item2schema() -> list[MetaCatalogue2Item2Schema]:
    """Extract `Catalogue2Item2Schema` table."""
    return extract_sql_table(MetaCatalogue2Item2Schema)


@asset(group_name="ifsg")
def meta_disease() -> list[MetaDisease]:
    """Extract `Disease` table."""
    return extract_sql_table(MetaDisease)


@asset(group_name="ifsg")
def meta_field() -> list[MetaField]:
    """Extract `Field` table."""
    return extract_sql_table(MetaField)


@asset(group_name="ifsg")
def filtered_empty_statement_area_group(meta_field: list[MetaField]) -> list[MetaField]:
    """Filter Metafield list for empty statement_area_group."""
    return filter_empty_statement_area_group(meta_field)


@asset(group_name="ifsg")
def filtered_variables(
    meta_field: list[MetaField],
    id_types_of_diseases: list[int],
) -> list[MetaField]:
    """Filter MetaField list."""
    return filter_variables(meta_field, id_types_of_diseases)


@asset(group_name="ifsg")
def meta_item() -> list[MetaItem]:
    """Extract `Item` table."""
    return extract_sql_table(MetaItem)


@asset(group_name="ifsg")
def meta_schema2field() -> list[MetaSchema2Field]:
    """Extract `Schema2Field` table."""
    return extract_sql_table(MetaSchema2Field)


@asset(group_name="ifsg")
def meta_schema2type() -> list[MetaSchema2Type]:
    """Extract `Schema2Type` table."""
    return extract_sql_table(MetaSchema2Type)


@asset(group_name="ifsg")
def id_types_of_diseases(
    meta_schema2type: list[MetaSchema2Type], meta_type: list[MetaType]
) -> list[int]:
    """Extract id_types that correspond to a disease."""
    return filter_id_type_of_diseases(meta_schema2type, meta_type)


@asset(group_name="ifsg")
def meta_type() -> list[MetaType]:
    """Extract `Type` table."""
    return extract_sql_table(MetaType)


@asset(group_name="ifsg")
def resource_disease() -> dict[str, Any]:
    """Extract `resource_disease` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "resource_disease.yaml")


@asset(group_name="ifsg")
def resource_parent() -> dict[str, Any]:
    """Extract `resource_parent` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "resource_parent.yaml")


@asset(group_name="ifsg")
def resource_state() -> dict[str, Any]:
    """Extract `resource_state` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "resource_state.yaml")


@asset(group_name="ifsg")
def ifsg_variable_group() -> dict[str, Any]:
    """Extract `ifsg_variable_group` default values."""
    settings = Settings.get()
    return load_yaml(settings.ifsg.mapping_path / "variable-group.yaml")


@asset(group_name="ifsg")
def extracted_ifsg_resource_parent(
    resource_parent: dict[str, Any],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedResource:
    """Extracted and loaded ifsg resource parent."""
    mex_resource_parent = transform_resource_parent_to_mex_resource(
        ResourceMapping.model_validate(resource_parent),
        extracted_primary_sources_ifsg,
        unit_stable_target_ids_by_synonym,
    )

    load([mex_resource_parent])

    return mex_resource_parent


@asset(group_name="ifsg")
def extracted_ifsg_resource_state(
    resource_state: dict[str, Any],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedResource]:
    """Extracted and loaded ifsg resource disease."""
    mex_resource_state = transform_resource_state_to_mex_resource(
        ResourceMapping.model_validate(resource_state),
        extracted_ifsg_resource_parent,
        extracted_primary_sources_ifsg,
        unit_stable_target_ids_by_synonym,
    )
    load(mex_resource_state)

    return mex_resource_state


@asset(group_name="ifsg")
def extracted_ifsg_resource_disease(  # noqa: PLR0913
    resource_disease: dict[str, Any],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_ifsg_resource_state: list[ExtractedResource],
    meta_disease: list[MetaDisease],
    meta_type: list[MetaType],
    id_types_of_diseases: list[int],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Extracted and loaded ifsg resource disease."""
    mex_resource_disease = transform_resource_disease_to_mex_resource(
        ResourceMapping.model_validate(resource_disease),
        extracted_ifsg_resource_parent,
        extracted_ifsg_resource_state,
        meta_disease,
        meta_type,
        id_types_of_diseases,
        extracted_primary_sources_ifsg,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
    )
    load(mex_resource_disease)

    return mex_resource_disease


@asset(group_name="ifsg")
def extracted_ifsg_variable_group(
    ifsg_variable_group: dict[str, Any],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    filtered_empty_statement_area_group: list[MetaField],
    id_types_of_diseases: list[int],
) -> list[ExtractedVariableGroup]:
    """Extracted and loaded ifsg variable group."""
    extracted_variable_group = transform_ifsg_data_to_mex_variable_group(
        VariableGroupMapping.model_validate(ifsg_variable_group),
        extracted_ifsg_resource_disease,
        extracted_primary_sources_ifsg,
        filtered_empty_statement_area_group,
        id_types_of_diseases,
    )
    load(extracted_variable_group)

    return extracted_variable_group


@asset(group_name="ifsg")
def extracted_ifsg_variable(  # noqa: PLR0913
    filtered_variables: list[MetaField],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_ifsg_variable_group: list[ExtractedVariableGroup],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    meta_catalogue2item: list[MetaCatalogue2Item],
    meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    meta_item: list[MetaItem],
    meta_datatype: list[MetaDataType],
    meta_schema2field: list[MetaSchema2Field],
) -> None:
    """Extracted and loaded ifsg variable."""
    extracted_variables = transform_ifsg_data_to_mex_variables(
        filtered_variables,
        extracted_ifsg_resource_disease,
        extracted_ifsg_variable_group,
        extracted_primary_sources_ifsg,
        meta_catalogue2item,
        meta_catalogue2item2schema,
        meta_item,
        meta_datatype,
        meta_schema2field,
    )
    load(extracted_variables)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the IFSG extractor job in-process."""
    run_job_in_process("ifsg")

from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, asset

from mex.common.cli import entrypoint
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.grippeweb.extract import (
    extract_columns_by_table_and_column_name,
    extract_grippeweb_ldap_person_ids_by_query,
    extract_grippeweb_organizations,
    extract_ldap_actors_for_functional_accounts,
)
from mex.extractors.grippeweb.transform import (
    transform_grippeweb_access_platform_to_extracted_access_platform,
    transform_grippeweb_resource_mappings_to_extracted_resources,
    transform_grippeweb_variable_group_to_extracted_variable_groups,
    transform_grippeweb_variable_to_extracted_variables,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="grippeweb")
def grippeweb_columns() -> dict[str, dict[str, list[Any]]]:
    """Extract Grippeweb SQL Server columns."""
    return extract_columns_by_table_and_column_name()


@asset(group_name="grippeweb")
def grippeweb_access_platform() -> dict[str, Any]:
    """Extract Grippeweb `access_platform` default values."""
    settings = Settings.get()
    return load_yaml(
        settings.grippeweb.mapping_path / "access-platform.yaml",
    )


@asset(group_name="grippeweb")
def grippeweb_resource_mappings() -> list[dict[str, Any]]:
    """Extract Grippeweb resource mappings."""
    settings = Settings.get()
    return [
        load_yaml(file)
        for file in Path(settings.grippeweb.mapping_path).glob("resource_*.yaml")
    ]


@asset(group_name="grippeweb")
def grippeweb_variable() -> dict[str, Any]:
    """Extract Grippeweb `variable` default values."""
    settings = Settings.get()
    return load_yaml(settings.grippeweb.mapping_path / "variable.yaml")


@asset(group_name="grippeweb")
def grippeweb_variable_group() -> dict[str, Any]:
    """Extract Grippeweb `variable_group` default values."""
    settings = Settings.get()
    return load_yaml(settings.grippeweb.mapping_path / "variable-group.yaml")


@asset(group_name="grippeweb")
def grippeweb_merged_contact_point_id_by_email(
    grippeweb_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedContactPointIdentifier]:
    """Extract ldap actors for grippeweb from ldap and transform them to contact points and load them to sinks."""  # noqa: E501
    return extract_ldap_actors_for_functional_accounts(
        [ResourceMapping.model_validate(r) for r in grippeweb_resource_mappings]
    )


@asset(group_name="grippeweb")
def grippeweb_extracted_persons(
    grippeweb_resource_mappings: list[dict[str, Any]],
    grippeweb_access_platform: dict[str, Any],
) -> dict[str, MergedPersonIdentifier]:
    """Extract ldap persons for grippeweb from ldap and transform them to mex persons and load them to sinks."""  # noqa: E501
    return extract_grippeweb_ldap_person_ids_by_query(
        [ResourceMapping.model_validate(r) for r in grippeweb_resource_mappings],
        AccessPlatformMapping.model_validate(grippeweb_access_platform),
    )


@asset(group_name="grippeweb")
def grippeweb_merged_organization_ids_by_query_str(
    grippeweb_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for grippeweb from wikidata and group them by query."""
    return extract_grippeweb_organizations(
        [ResourceMapping.model_validate(r) for r in grippeweb_resource_mappings]
    )


@asset(group_name="grippeweb")
def grippeweb_extracted_access_platform(
    grippeweb_access_platform: dict[str, Any],
    grippeweb_extracted_persons: dict[str, MergedPersonIdentifier],
) -> ExtractedAccessPlatform:
    """Transform Grippeweb mappings to extracted access platform and load to sinks."""
    grippeweb_extracted_access_platform = (
        transform_grippeweb_access_platform_to_extracted_access_platform(
            AccessPlatformMapping.model_validate(grippeweb_access_platform),
            grippeweb_extracted_persons,
        )
    )
    load([grippeweb_extracted_access_platform])
    return grippeweb_extracted_access_platform


@asset(group_name="grippeweb")
def grippeweb_extracted_parent_resource(
    grippeweb_resource_mappings: list[dict[str, Any]],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    grippeweb_extracted_persons: dict[str, MergedPersonIdentifier],
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
) -> ExtractedResource:
    """Transform Grippeweb default values to extracted resources and load to sinks."""
    parent_resource, child_resource = (
        transform_grippeweb_resource_mappings_to_extracted_resources(
            [ResourceMapping.model_validate(r) for r in grippeweb_resource_mappings],
            grippeweb_extracted_access_platform,
            grippeweb_extracted_persons,
            grippeweb_merged_organization_ids_by_query_str,
            grippeweb_merged_contact_point_id_by_email,
        )
    )
    load([parent_resource])
    load([child_resource])
    return parent_resource


@asset(group_name="grippeweb")
def grippeweb_extracted_variable_groups(
    grippeweb_variable_group: dict[str, Any],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform Grippeweb values to extracted variable groups and load to sinks."""
    extracted_variable_groups = (
        transform_grippeweb_variable_group_to_extracted_variable_groups(
            VariableGroupMapping.model_validate(grippeweb_variable_group),
            grippeweb_columns,
            grippeweb_extracted_parent_resource,
        )
    )
    load(extracted_variable_groups)
    return extracted_variable_groups


@asset(group_name="grippeweb", metadata={"entity_type": "variable"})
def grippeweb_extracted_variables(
    context: AssetExecutionContext,
    grippeweb_variable: dict[str, Any],
    grippeweb_extracted_variable_groups: list[ExtractedVariableGroup],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform Grippeweb default values to extracted variables and load to sinks."""
    extracted_variables = transform_grippeweb_variable_to_extracted_variables(
        VariableMapping.model_validate(grippeweb_variable),
        grippeweb_extracted_variable_groups,
        grippeweb_columns,
        grippeweb_extracted_parent_resource,
    )
    load(extracted_variables)
    context.add_output_metadata({"num_items": len(extracted_variables)})
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the Grippeweb extractor job in-process."""
    run_job_in_process("grippeweb")

from pathlib import Path
from typing import Any

from mex.common.cli import entrypoint
from mex.common.ldap.transform import (
    transform_ldap_actors_to_mex_contact_points,
    transform_ldap_persons_to_mex_persons,
)
from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
)
from mex.common.types import (
    Email,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.grippeweb.extract import (
    extract_columns_by_table_and_column_name,
    extract_grippeweb_organizations,
    extract_ldap_actors_for_functional_accounts,
    extract_ldap_persons,
)
from mex.extractors.grippeweb.transform import (
    transform_grippeweb_access_platform_to_extracted_access_platform,
    transform_grippeweb_resource_mappings_to_extracted_resources,
    transform_grippeweb_variable_group_to_extracted_variable_groups,
    transform_grippeweb_variable_to_extracted_variables,
)
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.mapping.transform import (
    transform_mapping_data_to_model,
    transform_mapping_data_to_models,
)
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.sumo.transform import get_contact_merged_ids_by_emails


@asset(group_name="grippeweb", deps=["extracted_primary_source_mex"])
def extracted_primary_source_grippeweb(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return Grippeweb primary source."""
    (extracted_primary_sources_grippeweb,) = get_primary_sources_by_name(
        extracted_primary_sources, "grippeweb"
    )
    load([extracted_primary_sources_grippeweb])

    return extracted_primary_sources_grippeweb


@asset(group_name="grippeweb")
def grippeweb_columns() -> dict[str, dict[str, list[Any]]]:
    """Extract Grippeweb SQL Server columns."""
    return extract_columns_by_table_and_column_name()


@asset(group_name="grippeweb")
def grippeweb_access_platform() -> dict[str, Any]:
    """Extract Grippeweb `access_platform` default values."""
    settings = Settings.get()
    return extract_mapping_data(
        settings.grippeweb.mapping_path / "access-platform.yaml",
    )


@asset(group_name="grippeweb")
def grippeweb_resource_mappings() -> list[dict[str, Any]]:
    """Extract Grippeweb resource mappings."""
    settings = Settings.get()
    return [
        extract_mapping_data(file)
        for file in Path(settings.grippeweb.mapping_path).glob("resource_*.yaml")
    ]


@asset(group_name="grippeweb")
def grippeweb_variable() -> dict[str, Any]:
    """Extract Grippeweb `variable` default values."""
    settings = Settings.get()
    return extract_mapping_data(settings.grippeweb.mapping_path / "variable.yaml")


@asset(group_name="grippeweb")
def grippeweb_variable_group() -> dict[str, Any]:
    """Extract Grippeweb `variable_group` default values."""
    settings = Settings.get()
    return extract_mapping_data(settings.grippeweb.mapping_path / "variable-group.yaml")


@asset(group_name="grippeweb")
def extracted_mex_functional_units_grippeweb(
    grippeweb_resource_mappings: list[dict[str, Any]],
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> dict[Email, MergedContactPointIdentifier]:
    """Extract ldap actors for grippeweb from ldap and transform them to contact points and load them to sinks."""  # noqa: E501
    ldap_actors = extract_ldap_actors_for_functional_accounts(
        transform_mapping_data_to_models(grippeweb_resource_mappings, ExtractedResource)
    )
    mex_actors_resources = list(
        transform_ldap_actors_to_mex_contact_points(
            ldap_actors, extracted_primary_source_ldap
        )
    )
    load(mex_actors_resources)
    return get_contact_merged_ids_by_emails(mex_actors_resources)


@asset(group_name="grippeweb")
def extracted_mex_persons_grippeweb(
    grippeweb_resource_mappings: list[dict[str, Any]],
    grippeweb_access_platform: dict[str, Any],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> list[ExtractedPerson]:
    """Extract ldap persons for grippeweb from ldap and transform them to mex persons and load them to sinks."""  # noqa: E501
    ldap_persons = extract_ldap_persons(
        transform_mapping_data_to_models(
            grippeweb_resource_mappings, ExtractedResource
        ),
        transform_mapping_data_to_model(
            grippeweb_access_platform, ExtractedAccessPlatform
        ),
    )
    mex_persons = list(
        transform_ldap_persons_to_mex_persons(
            ldap_persons, extracted_primary_source_ldap, extracted_organizational_units
        )
    )
    load(mex_persons)
    return mex_persons


@asset(group_name="grippeweb")
def grippeweb_organization_ids_by_query_string(
    grippeweb_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for grippeweb from wikidata and group them by query."""
    return extract_grippeweb_organizations(
        transform_mapping_data_to_models(grippeweb_resource_mappings, ExtractedResource)
    )


@asset(group_name="grippeweb")
def extracted_access_platform_grippeweb(
    grippeweb_access_platform: dict[str, Any],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
) -> ExtractedAccessPlatform:
    """Transform Grippeweb mappings to extracted access platform and load to sinks."""
    extracted_access_platform_grippeweb = (
        transform_grippeweb_access_platform_to_extracted_access_platform(
            transform_mapping_data_to_model(
                grippeweb_access_platform, ExtractedAccessPlatform
            ),
            unit_stable_target_ids_by_synonym,
            extracted_primary_source_grippeweb,
            extracted_mex_persons_grippeweb,
        )
    )
    load([extracted_access_platform_grippeweb])
    return extracted_access_platform_grippeweb


@asset(group_name="grippeweb")
def grippeweb_extracted_resource_dict(
    grippeweb_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_access_platform_grippeweb: ExtractedAccessPlatform,
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
) -> dict[str, ExtractedResource]:
    """Transform Grippeweb default values to extracted resources and load to sinks."""
    extracted_resources = transform_grippeweb_resource_mappings_to_extracted_resources(
        transform_mapping_data_to_models(
            grippeweb_resource_mappings, ExtractedResource
        ),
        unit_stable_target_ids_by_synonym,
        extracted_access_platform_grippeweb,
        extracted_primary_source_grippeweb,
        extracted_mex_persons_grippeweb,
        grippeweb_organization_ids_by_query_string,
        extracted_mex_functional_units_grippeweb,
    )
    load(list(extracted_resources.values()))
    return extracted_resources


@asset(group_name="grippeweb")
def grippeweb_extracted_variable_group(
    grippeweb_variable_group: dict[str, Any],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_resource_dict: dict[str, ExtractedResource],
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
) -> list[ExtractedVariableGroup]:
    """Transform Grippeweb values to extracted variable groups and load to sinks."""
    extracted_variable_groups = (
        transform_grippeweb_variable_group_to_extracted_variable_groups(
            transform_mapping_data_to_model(
                grippeweb_variable_group, ExtractedVariableGroup
            ),
            grippeweb_columns,
            grippeweb_extracted_resource_dict,
            extracted_primary_source_grippeweb,
        )
    )
    load(extracted_variable_groups)
    return extracted_variable_groups


@asset(group_name="grippeweb")
def grippeweb_extracted_variable(
    grippeweb_variable: dict[str, Any],
    grippeweb_extracted_variable_group: list[ExtractedVariableGroup],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_resource_dict: dict[str, ExtractedResource],
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
) -> None:
    """Transform Grippeweb default values to extracted variables and load to sinks."""
    extracted_variables = transform_grippeweb_variable_to_extracted_variables(
        transform_mapping_data_to_model(grippeweb_variable, ExtractedVariable),
        grippeweb_extracted_variable_group,
        grippeweb_columns,
        grippeweb_extracted_resource_dict,
        extracted_primary_source_grippeweb,
    )
    load(extracted_variables)


@entrypoint(Settings)
def run() -> None:
    """Run the Grippeweb extractor job in-process."""
    run_job_in_process("grippeweb")

from pathlib import Path
from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.transform import transform_ldap_persons_to_extracted_persons
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml
from mex.extractors.voxco.extract import (
    extract_ldap_persons_voxco,
    extract_voxco_organizations,
    extract_voxco_variables,
)
from mex.extractors.voxco.model import VoxcoVariable
from mex.extractors.voxco.transform import (
    transform_voxco_resource_mappings_to_extracted_resources,
    transform_voxco_variable_mappings_to_extracted_variables,
)


@asset(group_name="voxco")
def voxco_variables_by_name_str() -> dict[str, list[VoxcoVariable]]:
    """Extract voxco variables by json file names."""
    return extract_voxco_variables()


@asset(group_name="voxco")
def voxco_resource_mappings() -> list[dict[str, Any]]:
    """Extract voxco resource mappings."""
    settings = Settings.get()
    return [
        load_yaml(file)
        for file in Path(settings.voxco.mapping_path).glob("resource_*.yaml")
    ]


@asset(group_name="voxco")
def voxco_merged_organization_ids_by_query_string(
    voxco_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract and load voxco organizations and group them by query."""
    return extract_voxco_organizations(
        [ResourceMapping.model_validate(r) for r in voxco_resource_mappings]
    )


@asset(group_name="voxco")
def voxco_extracted_persons(
    voxco_resource_mappings: list[dict[str, Any]],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedPerson]:
    """Extract ldap persons for voxco, transform them and load them to sinks."""
    ldap_persons = extract_ldap_persons_voxco(
        [ResourceMapping.model_validate(r) for r in voxco_resource_mappings]
    )
    mex_persons = transform_ldap_persons_to_extracted_persons(
        ldap_persons,
        get_extracted_primary_source_id_by_name("ldap"),
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_persons)
    return mex_persons


@asset(group_name="voxco")
def voxco_extracted_resources_by_str(  # noqa: PLR0913
    voxco_resource_mappings: list[dict[str, Any]],
    voxco_merged_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    voxco_extracted_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[
        str, list[MergedOrganizationalUnitIdentifier]
        ],
    extracted_organization_rki: ExtractedOrganization,
    international_projects_extracted_activities: list[ExtractedActivity],
) -> dict[str, ExtractedResource]:
    """Transform mex resources, load to them to the sinks and return."""
    mex_resources = transform_voxco_resource_mappings_to_extracted_resources(
        [ResourceMapping.model_validate(r) for r in voxco_resource_mappings],
        voxco_merged_organization_ids_by_query_string,
        voxco_extracted_persons,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
        international_projects_extracted_activities,
    )
    load(mex_resources.values())

    return mex_resources


@asset(group_name="voxco")
def voxco_extracted_variables(
    voxco_extracted_resources_by_str: dict[str, ExtractedResource],
    voxco_variables_by_name_str: dict[str, list[VoxcoVariable]],
) -> list[ExtractedVariable]:
    """Transform voxco variables and load them to the sinks."""
    extracted_variables = transform_voxco_variable_mappings_to_extracted_variables(
        voxco_extracted_resources_by_str,
        voxco_variables_by_name_str,
    )
    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the voxco extractor job in-process."""
    run_job_in_process("voxco")

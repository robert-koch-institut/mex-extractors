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
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
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


@asset(group_name="voxco", deps=["extracted_primary_source_mex"])
def voxco_extracted_primary_source(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return voxco primary source."""
    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "voxco"
    )
    load([extracted_primary_source])

    return extracted_primary_source


@asset(group_name="voxco")
def voxco_variables() -> dict[str, list[VoxcoVariable]]:
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
def voxco_organization_stable_target_id_by_query(
    voxco_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract and load voxco organizations and group them by query."""
    return extract_voxco_organizations(
        [ResourceMapping.model_validate(r) for r in voxco_resource_mappings]
    )


@asset(group_name="voxco")
def voxco_persons(
    voxco_resource_mappings: list[dict[str, Any]],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedPerson]:
    """Extract ldap persons for voxco, transform them and load them to sinks."""
    ldap_persons = extract_ldap_persons_voxco(
        [ResourceMapping.model_validate(r) for r in voxco_resource_mappings]
    )
    mex_persons = transform_ldap_persons_to_extracted_persons(
        ldap_persons,
        extracted_primary_source_ldap,
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_persons)
    return mex_persons


@asset(group_name="voxco")
def voxco_resources(  # noqa: PLR0913
    voxco_resource_mappings: list[dict[str, Any]],
    voxco_organization_stable_target_id_by_query: dict[
        str, MergedOrganizationIdentifier
    ],
    voxco_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    voxco_extracted_primary_source: ExtractedPrimarySource,
    international_projects_extracted_activities: list[ExtractedActivity],
) -> dict[str, ExtractedResource]:
    """Transform mex resources, load to them to the sinks and return."""
    mex_resources = transform_voxco_resource_mappings_to_extracted_resources(
        [ResourceMapping.model_validate(r) for r in voxco_resource_mappings],
        voxco_organization_stable_target_id_by_query,
        voxco_persons,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
        voxco_extracted_primary_source,
        international_projects_extracted_activities,
    )
    load(mex_resources.values())

    return mex_resources


@asset(group_name="voxco")
def voxco_extracted_variables(
    voxco_resources: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
    voxco_extracted_primary_source: ExtractedPrimarySource,
) -> list[ExtractedVariable]:
    """Transform voxco variables and load them to the sinks."""
    extracted_variables = transform_voxco_variable_mappings_to_extracted_variables(
        voxco_resources, voxco_variables, voxco_extracted_primary_source
    )
    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the voxco extractor job in-process."""
    run_job_in_process("voxco")

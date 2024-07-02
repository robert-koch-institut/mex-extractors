from pathlib import Path
from typing import Any

from mex.common.cli import entrypoint
from mex.common.ldap.transform import (
    transform_ldap_persons_to_mex_persons,
)
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
)
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.mapping.extract import extract_mapping_data
from mex.pipeline import asset, run_job_in_process
from mex.sinks import load
from mex.voxco.extract import (
    extract_ldap_persons_voxco,
    extract_voxco_organizations,
    extract_voxco_variables,
)
from mex.voxco.model import VoxcoVariable
from mex.voxco.settings import VoxcoSettings
from mex.voxco.transform import (
    transform_voxco_resource_mappings_to_extracted_resources,
    transform_voxco_variable_mappings_to_extracted_variables,
)
from mex.wikidata.extract import (
    get_merged_organization_id_by_query_with_transform_and_load,
)


@asset(group_name="voxco", deps=["extracted_primary_source_mex"])
def extracted_primary_source_voxco(
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
    settings = VoxcoSettings.get()
    return [
        extract_mapping_data(file, ExtractedResource)
        for file in Path(settings.mapping_path).glob("resource_*.yaml")
    ]


@asset(group_name="voxco")
def organization_stable_target_id_by_query_voxco(
    voxco_resource_mappings: list[dict[str, Any]],
    extracted_primary_source_wikidata: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract and load voxco organizations and group them by query."""
    voxco_organizations = extract_voxco_organizations(voxco_resource_mappings)

    return get_merged_organization_id_by_query_with_transform_and_load(
        voxco_organizations, extracted_primary_source_wikidata
    )


@asset(group_name="voxco")
def extracted_mex_persons_voxco(
    voxco_resource_mappings: list[dict[str, Any]],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> list[ExtractedPerson]:
    """Extract ldap persons for voxco, transform them and load them to sinks."""
    ldap_persons = extract_ldap_persons_voxco(voxco_resource_mappings)
    mex_persons = list(
        transform_ldap_persons_to_mex_persons(
            ldap_persons, extracted_primary_source_ldap, extracted_organizational_units
        )
    )
    load(mex_persons)
    return mex_persons


@asset(group_name="voxco")
def extracted_voxco_resources(
    voxco_resource_mappings: list[dict[str, Any]],
    organization_stable_target_id_by_query_voxco: dict[
        str, MergedOrganizationIdentifier
    ],
    extracted_mex_persons_voxco: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_source_voxco: ExtractedPrimarySource,
    extracted_international_projects_activities: list[ExtractedActivity],
) -> dict[str, ExtractedResource]:
    """Transform mex resources, load to them to the sinks and return."""
    mex_resources = transform_voxco_resource_mappings_to_extracted_resources(
        voxco_resource_mappings,
        organization_stable_target_id_by_query_voxco,
        extracted_mex_persons_voxco,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
        extracted_primary_source_voxco,
        extracted_international_projects_activities,
    )
    load(mex_resources.values())

    return mex_resources


@asset(group_name="voxco")
def extracted_variables_voxco(
    extracted_voxco_resources: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
    extracted_primary_source_voxco: ExtractedPrimarySource,
) -> None:
    """Transform voxco variables and load them to the sinks."""
    extracted_variables = transform_voxco_variable_mappings_to_extracted_variables(
        extracted_voxco_resources, voxco_variables, extracted_primary_source_voxco
    )
    load(extracted_variables)


@entrypoint(VoxcoSettings)
def run() -> None:
    """Run the voxco extractor job in-process."""
    run_job_in_process("voxco")

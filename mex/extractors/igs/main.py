from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.transform import transform_ldap_actor_to_mex_contact_point
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedPrimarySource,
    ResourceMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
)
from mex.extractors.igs.extract import (
    extract_igs_info,
    extract_igs_schemas,
    extract_ldap_actors_by_mail,
)
from mex.extractors.igs.filter import filter_creation_schemas
from mex.extractors.igs.model import IGSInfo, IGSSchema
from mex.extractors.igs.transform import (
    transform_igs_access_platform,
    transform_igs_schemas_to_resources,
    transform_igs_schemas_to_variables,
    transformed_igs_schemas_to_variable_group,
)
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="igs", deps=["extracted_primary_source_mex"])
def extracted_primary_source_igs(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return IGS primary source."""
    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "igs"
    )
    load([extracted_primary_source])
    return extracted_primary_source


@asset(group_name="igs")
def igs_schemas() -> dict[str, IGSSchema]:
    """Extract from IGS schemas."""
    return extract_igs_schemas()


@asset(group_name="igs")
def igs_info() -> IGSInfo:
    """Extract from IGS info."""
    return extract_igs_info()


@asset(group_name="igs")
def igs_resource_mapping() -> dict[str, Any]:
    """Extract IGS resource mapping."""
    settings = Settings.get()
    return load_yaml(settings.igs.mapping_path / "resource.yaml")


@asset(group_name="igs")
def igs_access_platform_mapping() -> dict[str, Any]:
    """Extract IGS access platform mapping."""
    settings = Settings.get()
    return load_yaml(settings.igs.mapping_path / "access-platform.yaml")


@asset(group_name="igs")
def extracted_igs_contact_points_by_mail(
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_resource_mapping: dict[str, Any],
    igs_access_platform_mapping: dict[str, Any],
) -> dict[str, ExtractedContactPoint]:
    """Extract IGS contact points by mail."""
    igs_actors_by_mail = extract_ldap_actors_by_mail(
        ResourceMapping.model_validate(igs_resource_mapping),
        AccessPlatformMapping.model_validate(igs_access_platform_mapping),
    )
    extracted_contact_points = {
        mail: transform_ldap_actor_to_mex_contact_point(
            actor, extracted_primary_source_igs
        )
        for mail, actor in igs_actors_by_mail.items()
    }
    load(extracted_contact_points.values())
    return extracted_contact_points


@asset(group_name="igs")
def extracted_igs_resource_ids_by_pathogen(  # noqa: PLR0913
    igs_schemas: dict[str, IGSSchema],
    igs_info: IGSInfo,
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_resource_mapping: dict[str, Any],
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> dict[str, MergedResourceIdentifier]:
    """Transform IGS resource from IGS schemas."""
    extracted_resources = transform_igs_schemas_to_resources(
        igs_schemas,
        igs_info,
        extracted_primary_source_igs,
        ResourceMapping.model_validate(igs_resource_mapping),
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )
    load(extracted_resources)
    return {
        resource.identifierInPrimarySource: resource.stableTargetId
        for resource in extracted_resources
    }


@asset(group_name="igs")
def extracted_igs_access_platform(
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_access_platform_mapping: dict[str, Any],
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedAccessPlatform:
    """Transform IGS access platform from mapping."""
    return transform_igs_access_platform(
        extracted_primary_source_igs,
        AccessPlatformMapping.model_validate(igs_access_platform_mapping),
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )


@asset(group_name="igs")
def extracted_igs_variable_group_ids_by_identifier_in_primary_source(
    igs_schemas: dict[str, IGSSchema],
    extracted_igs_resource_ids_by_pathogen: dict[str, MergedResourceIdentifier],
    extracted_primary_source_igs: ExtractedPrimarySource,
) -> dict[str, MergedVariableGroupIdentifier]:
    """Filter and transform IGS schema to extracted variable group."""
    settings = Settings.get()
    variable_group_mapping = VariableGroupMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "variable-group.yaml")
    )
    filtered_schemas = filter_creation_schemas(igs_schemas)
    extracted_variable_groups = transformed_igs_schemas_to_variable_group(
        filtered_schemas,
        variable_group_mapping,
        extracted_igs_resource_ids_by_pathogen,
        extracted_primary_source_igs,
    )
    load(extracted_variable_groups)
    return {
        group.identifierInPrimarySource: group.stableTargetId
        for group in extracted_variable_groups
    }


@asset(group_name="igs")
def extracted_igs_variables(
    igs_schemas: dict[str, IGSSchema],
    extracted_igs_resource_ids_by_pathogen: dict[str, MergedResourceIdentifier],
    extracted_primary_source_igs: ExtractedPrimarySource,
    extracted_igs_variable_group_ids_by_identifier_in_primary_source: dict[
        str, MergedVariableGroupIdentifier
    ],
) -> None:
    """Transform igs schemas to extracted variables."""
    settings = Settings.get()
    variable_mapping = VariableMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "access-platform.yaml")
    )
    load(
        transform_igs_schemas_to_variables(
            igs_schemas,
            extracted_igs_resource_ids_by_pathogen,
            extracted_primary_source_igs,
            extracted_igs_variable_group_ids_by_identifier_in_primary_source,
            variable_mapping,
        )
    )


@entrypoint(Settings)
def run() -> None:
    """Run the IGS extractor job in-process."""
    run_job_in_process("igs")

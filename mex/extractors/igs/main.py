from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.transform import (
    transform_ldap_functional_account_to_extracted_contact_point,
)
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedOrganization,
    ResourceMapping,
)
from mex.common.types import (
    MergedResourceIdentifier,
)
from mex.extractors.igs.extract import (
    extract_endpoint_counts,
    extract_igs_info,
    extract_igs_schemas,
    extract_ldap_actors_by_mail,
)
from mex.extractors.igs.model import IGSInfo, IGSSchema
from mex.extractors.igs.transform import (
    transform_igs_access_platform,
    transform_igs_extracted_resource,
)
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="igs")
def igs_info() -> IGSInfo:
    """Extract from IGS info."""
    return extract_igs_info()


@asset(group_name="igs")
def igs_schemas() -> dict[str, IGSSchema]:
    """Extract from IGS schemas."""
    return extract_igs_schemas()


@asset(group_name="igs")
def igs_resource_mapping() -> dict[str, Any]:
    """Extract IGS resource mapping."""
    settings = Settings.get()
    return load_yaml(settings.igs.mapping_path / "resource.yaml")


@asset(group_name="igs")
def igs_endpoint_counts(
    igs_resource_mapping: dict[str, Any], igs_schemas: dict[str, IGSSchema]
) -> dict[str, str]:
    """Extract endpoint counts from IGS API."""
    return extract_endpoint_counts(
        ResourceMapping.model_validate(igs_resource_mapping), igs_schemas
    )


@asset(group_name="igs")
def igs_access_platform_mapping() -> dict[str, Any]:
    """Extract IGS access platform mapping."""
    settings = Settings.get()
    return load_yaml(settings.igs.mapping_path / "access-platform.yaml")


@asset(group_name="igs")
def igs_extracted_contact_points_by_mail_str(
    igs_resource_mapping: dict[str, Any],
    igs_access_platform_mapping: dict[str, Any],
) -> dict[str, ExtractedContactPoint]:
    """Extract IGS contact points by mail."""
    igs_actors_by_mail = extract_ldap_actors_by_mail(
        ResourceMapping.model_validate(igs_resource_mapping),
        AccessPlatformMapping.model_validate(igs_access_platform_mapping),
    )
    extracted_contact_points = {
        mail: transform_ldap_functional_account_to_extracted_contact_point(
            actor, get_extracted_primary_source_id_by_name("ldap")
        )
        for mail, actor in igs_actors_by_mail.items()
    }
    load(extracted_contact_points.values())
    return extracted_contact_points


@asset(group_name="igs")
def igs_extracted_resource_id_by_pathogen(  # noqa: PLR0913
    igs_resource_mapping: dict[str, Any],
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
    igs_extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
    igs_schemas: dict[str, IGSSchema],
    igs_info: IGSInfo,
    igs_endpoint_counts: dict[str, str],
) -> dict[str, MergedResourceIdentifier]:
    """Transform IGS resource from IGS schemas."""
    extracted_resources = transform_igs_extracted_resource(
        ResourceMapping.model_validate(igs_resource_mapping),
        igs_extracted_contact_points_by_mail_str,
        igs_extracted_access_platform,
        extracted_organization_rki,
        igs_schemas,
        igs_info,
        igs_endpoint_counts,
    )
    load(extracted_resources.values())
    return {
        pathogen: resource.stableTargetId
        for pathogen, resource in extracted_resources.items()
    }


@asset(group_name="igs")
def igs_extracted_access_platform(
    igs_access_platform_mapping: dict[str, Any],
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
) -> ExtractedAccessPlatform:
    """Transform IGS access platform from mapping."""
    extracted_access_platform = transform_igs_access_platform(
        AccessPlatformMapping.model_validate(igs_access_platform_mapping),
        igs_extracted_contact_points_by_mail_str,
    )
    load([extracted_access_platform])
    return extracted_access_platform


# TODO(EH): update in MX-2162
'''@asset(group_name="igs")
def igs_extracted_variable_group_ids_by_identifier_in_primary_source(
    igs_schemas: dict[str, IGSSchema],
    igs_extracted_resource_id_by_pathogen: MergedResourceIdentifier,
) -> dict[str, MergedVariableGroupIdentifier]:
    """Filter and transform IGS schema to extracted variable group."""
    filtered_schemas = filter_creation_schemas(igs_schemas)
    extracted_variable_groups = transformed_igs_schemas_to_variable_group(
        filtered_schemas,
        igs_extracted_resource_id_by_pathogen,
    )
    load(extracted_variable_groups)
    return {
        group.identifierInPrimarySource: group.stableTargetId
        for group in extracted_variable_groups
    }


@asset(group_name="igs")
def igs_extracted_variables(
    igs_schemas: dict[str, IGSSchema],
    igs_extracted_resource_id_by_pathogen: MergedResourceIdentifier,
    igs_extracted_variable_group_ids_by_identifier_in_primary_source: dict[
        str, MergedVariableGroupIdentifier
    ],
) -> None:
    """Transform igs schemas to extracted variables."""
    settings = Settings.get()
    variable_mapping = VariableMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "variable.yaml")
    )
    variable_pathogen_mapping = VariableMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "variable_pathogen.yaml")
    )
    load(
        transform_igs_schemas_to_variables(
            igs_schemas,
            igs_extracted_resource_id_by_pathogen,
            igs_extracted_variable_group_ids_by_identifier_in_primary_source,
            variable_mapping,
            variable_pathogen_mapping,
        )
    )'''


@entrypoint(Settings)
def run() -> None:
    """Run the IGS extractor job in-process."""
    run_job_in_process("igs")

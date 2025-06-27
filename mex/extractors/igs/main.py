from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.transform import transform_ldap_actor_to_mex_contact_point
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.igs.extract import (
    extract_igs_schemas,
    extract_ldap_actors_by_mail,
)
from mex.extractors.igs.model import IGSSchema
from mex.extractors.igs.transform import (
    transform_igs_access_platform,
    transform_igs_schemas_to_resources,
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
def extracted_igs_resources(
    igs_schemas: dict[str, IGSSchema],
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_resource_mapping: dict[str, Any],
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> dict[str, ExtractedResource]:
    """Transform IGS resource from IGS schemas."""
    return transform_igs_schemas_to_resources(
        igs_schemas,
        extracted_primary_source_igs,
        ResourceMapping.model_validate(igs_resource_mapping),
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )


@asset(group_name="igs")
def extracted_igs_access_platform(
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_access_platform_mapping: AccessPlatformMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedAccessPlatform:
    """Transform IGS access platform from mapping."""
    return transform_igs_access_platform(
        extracted_primary_source_igs,
        igs_access_platform_mapping,
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )


@entrypoint(Settings)
def run() -> None:
    """Run the IGS extractor job in-process."""
    run_job_in_process("igs")

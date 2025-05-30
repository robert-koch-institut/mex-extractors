from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_employee_ids
from mex.common.ldap.transform import transform_ldap_persons_to_mex_persons
from mex.common.models import (
    ActivityMapping,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.blueant.extract import (
    extract_blueant_organizations,
    extract_blueant_project_leaders,
    extract_blueant_sources,
)
from mex.extractors.blueant.filter import filter_and_log_blueant_sources
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.blueant.transform import (
    transform_blueant_sources_to_extracted_activities,
)
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="blueant", deps=["extracted_primary_source_mex"])
def extracted_primary_source_blueant(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return blueant primary source."""
    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "blueant"
    )
    load([extracted_primary_source])
    return extracted_primary_source


@asset(group_name="blueant")
def blueant_sources(
    extracted_primary_source_blueant: ExtractedPrimarySource,
) -> list[BlueAntSource]:
    """Extract from blueant sources and filter content."""
    sources = extract_blueant_sources()
    sources = filter_and_log_blueant_sources(
        sources, extracted_primary_source_blueant.stableTargetId
    )
    return filter_by_global_rules(
        extracted_primary_source_blueant.stableTargetId, sources
    )


@asset(group_name="blueant")
def blueant_project_leaders_by_employee_id(
    blueant_sources: list[BlueAntSource],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, list[MergedPersonIdentifier]]:
    """Transform LDAP persons to mex-persons with stable target ID and group them by employee ID."""  # noqa: E501
    ldap_project_leaders = list(extract_blueant_project_leaders(blueant_sources))
    mex_project_leaders = transform_ldap_persons_to_mex_persons(
        ldap_project_leaders,
        extracted_primary_source_ldap,
        extracted_organizational_units,
    )
    load(mex_project_leaders)
    return get_merged_ids_by_employee_ids(
        ldap_project_leaders, extracted_primary_source_ldap
    )


@asset(group_name="blueant")
def blueant_organization_ids_by_query_string(
    blueant_sources: list[BlueAntSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for blueant from wikidata and group them by query."""
    return extract_blueant_organizations(blueant_sources)


@asset(group_name="blueant")
def extracted_blueant_activities(
    blueant_sources: list[BlueAntSource],
    extracted_primary_source_blueant: ExtractedPrimarySource,
    blueant_project_leaders_by_employee_id: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    blueant_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> None:
    """Transform blueant sources to extracted activities and load them to the sinks."""
    settings = Settings.get()
    activity = ActivityMapping.model_validate(
        load_yaml(settings.blueant.mapping_path / "activity.yaml")
    )

    extracted_activities = transform_blueant_sources_to_extracted_activities(
        blueant_sources,
        extracted_primary_source_blueant,
        blueant_project_leaders_by_employee_id,
        unit_stable_target_ids_by_synonym,
        activity,
        blueant_organization_ids_by_query_string,
    )
    load(extracted_activities)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the blueant extractor job in-process."""
    run_job_in_process("blueant")

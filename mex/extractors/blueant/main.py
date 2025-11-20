from dagster import (
    AssetExecutionContext,
    AssetObservation,
    MetadataValue,
    Output,
    asset,
)

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_employee_ids
from mex.common.ldap.transform import transform_ldap_persons_to_extracted_persons
from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
)
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
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="blueant")
def blueant_sources() -> list[BlueAntSource]:
    """Extract from blueant sources and filter content."""
    sources = extract_blueant_sources()
    blueant_primary_source_id = get_extracted_primary_source_id_by_name("blueant")
    sources = filter_and_log_blueant_sources(sources)
    return filter_by_global_rules(blueant_primary_source_id, sources)


@asset(group_name="blueant")
def blueant_merged_person_id_by_employee_id(
    blueant_sources: list[BlueAntSource],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Transform LDAP persons to mex-persons with stable target ID and group them by employee ID."""  # noqa: E501
    ldap_project_leaders = extract_blueant_project_leaders(blueant_sources)
    mex_project_leaders = transform_ldap_persons_to_extracted_persons(
        ldap_project_leaders,
        get_extracted_primary_source_id_by_name("ldap"),
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_project_leaders)
    return get_merged_ids_by_employee_ids(
        ldap_project_leaders, get_extracted_primary_source_id_by_name("ldap")
    )


@asset(group_name="blueant")
def blueant_merged_organization_ids_by_query_str(
    blueant_sources: list[BlueAntSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for blueant from wikidata and group them by query."""
    return extract_blueant_organizations(blueant_sources)


def create_output(
    context: AssetExecutionContext,
    entity_type: str,
    num_items: int,
) -> Output[int]:
    """Creates Observation for asset key and an Output for values."""
    context.log_event(
        AssetObservation(
            asset_key=context.asset_key,
            metadata={"entity_type": MetadataValue.text(entity_type)},
        )
    )
    return Output(
        value=num_items,
        metadata={
            "num_items": MetadataValue.int(num_items),
        },
    )


@asset(
    group_name="blueant",
    metadata={"entity_type": "activity"},
)
def blueant_extracted_activities(
    context: AssetExecutionContext,
    blueant_sources: list[BlueAntSource],
    blueant_merged_person_id_by_employee_id: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    blueant_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
) -> Output[int]:
    """Transform blueant sources to extracted activities and load them to the sinks."""
    settings = Settings.get()
    activity = ActivityMapping.model_validate(
        load_yaml(settings.blueant.mapping_path / "activity.yaml")
    )

    extracted_activities = transform_blueant_sources_to_extracted_activities(
        blueant_sources,
        blueant_merged_person_id_by_employee_id,
        unit_stable_target_ids_by_synonym,
        activity,
        blueant_merged_organization_ids_by_query_str,
    )

    extracted_activities_list: list[ExtractedActivity] = list(extracted_activities)
    num_items = len(extracted_activities)
    load(extracted_activities)
    return create_output(
        context=context,
        entity_type=extracted_activities_list[0].stemType,
        num_items=num_items,
    )


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the blueant extractor job in-process."""
    run_job_in_process("blueant")

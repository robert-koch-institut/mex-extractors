from pathlib import Path
from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedActivity,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.odk.extract import (
    extract_odk_raw_data,
    get_external_partner_and_publisher_by_label,
)
from mex.extractors.odk.model import ODKData
from mex.extractors.odk.transform import (
    assign_resource_relations_and_load,
    transform_odk_data_to_extracted_variables,
    transform_odk_resources_to_mex_resources,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="odk", deps=["extracted_primary_source_mex"])
def extracted_primary_source_odk(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return odk primary source and load them to sinks."""
    (extracted_primary_source_odk,) = get_primary_sources_by_name(
        extracted_primary_sources, "odk"
    )
    load([extracted_primary_source_odk])
    return extracted_primary_source_odk


@asset(group_name="odk")
def odk_raw_data() -> list[ODKData]:
    """Extract odk raw data."""
    return extract_odk_raw_data()


@asset(group_name="odk")
def odk_resource_mappings() -> list[dict[str, Any]]:
    """Extract odk resource mappings."""
    settings = Settings.get()
    return [
        load_yaml(file)
        for file in Path(settings.odk.mapping_path).glob("resource_*.yaml")
    ]


@asset(group_name="odk")
def external_partner_and_publisher_by_label(
    odk_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract partner organizations and return their IDs grouped by query string."""
    return get_external_partner_and_publisher_by_label(
        [ResourceMapping.model_validate(r) for r in odk_resource_mappings]
    )


@asset(group_name="odk")
def extracted_resources_odk(  # noqa: PLR0913
    odk_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    external_partner_and_publisher_by_label: dict[str, MergedOrganizationIdentifier],
    extracted_international_projects_activities: list[ExtractedActivity],
    extracted_primary_source_mex: ExtractedPrimarySource,
    extracted_primary_source_odk: ExtractedPrimarySource,
) -> list[ExtractedResource]:
    """Transform odk resources to mex resource, load to sinks and return."""
    extracted_resources_tuple = transform_odk_resources_to_mex_resources(
        [ResourceMapping.model_validate(r) for r in odk_resource_mappings],
        unit_stable_target_ids_by_synonym,
        external_partner_and_publisher_by_label,
        extracted_international_projects_activities,
        extracted_primary_source_mex,
        extracted_primary_source_odk,
    )
    return assign_resource_relations_and_load(extracted_resources_tuple)


@asset(group_name="odk")
def extracted_variables_odk(
    extracted_resources_odk: list[ExtractedResource],
    odk_raw_data: list[ODKData],
    extracted_primary_source_odk: ExtractedPrimarySource,
) -> None:
    """Transform odk data to mex variables and load to sinks."""
    extracted_variables = transform_odk_data_to_extracted_variables(
        extracted_resources_odk,
        odk_raw_data,
        extracted_primary_source_odk,
    )

    load(extracted_variables)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the odk extractor job in-process."""
    run_job_in_process("odk")

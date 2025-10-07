from pathlib import Path
from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedActivity,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
    VariableMapping,
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
def odk_extracted_primary_source(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return odk primary source and load them to sinks."""
    (odk_extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "odk"
    )
    load([odk_extracted_primary_source])
    return odk_extracted_primary_source


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
def odk_external_partner_and_publisher_by_label(
    odk_resource_mappings: list[dict[str, Any]],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract partner organizations and return their IDs grouped by query string."""
    return get_external_partner_and_publisher_by_label(
        [ResourceMapping.model_validate(r) for r in odk_resource_mappings]
    )


@asset(group_name="odk")
def odk_extracted_resources(  # noqa: PLR0913
    odk_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    odk_external_partner_and_publisher_by_label: dict[
        str, MergedOrganizationIdentifier
    ],
    international_projects_extracted_activities: list[ExtractedActivity],
    extracted_primary_source_mex: ExtractedPrimarySource,
    odk_extracted_primary_source: ExtractedPrimarySource,
) -> list[ExtractedResource]:
    """Transform odk resources to mex resource, load to sinks and return."""
    extracted_resources_tuple = transform_odk_resources_to_mex_resources(
        [ResourceMapping.model_validate(r) for r in odk_resource_mappings],
        unit_stable_target_ids_by_synonym,
        odk_external_partner_and_publisher_by_label,
        international_projects_extracted_activities,
        extracted_primary_source_mex,
        odk_extracted_primary_source,
    )
    return assign_resource_relations_and_load(extracted_resources_tuple)


@asset(group_name="odk")
def odk_extracted_variables(
    odk_extracted_resources: list[ExtractedResource],
    odk_raw_data: list[ODKData],
    odk_extracted_primary_source: ExtractedPrimarySource,
) -> list[ExtractedVariable]:
    """Transform odk data to mex variables and load to sinks."""
    settings = Settings.get()
    variable_mapping = VariableMapping.model_validate(
        load_yaml(settings.odk.mapping_path / "variable.yaml")
    )

    extracted_variables = transform_odk_data_to_extracted_variables(
        odk_extracted_resources,
        odk_raw_data,
        odk_extracted_primary_source,
        variable_mapping,
    )

    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the odk extractor job in-process."""
    run_job_in_process("odk")

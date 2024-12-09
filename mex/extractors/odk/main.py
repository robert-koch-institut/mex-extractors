from pathlib import Path
from typing import Any

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedPrimarySource,
    ExtractedResource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.mapping.transform import transform_mapping_data_to_models
from mex.extractors.odk.extract import (
    extract_odk_raw_data,
    get_external_partner_and_publisher_by_label,
)
from mex.extractors.odk.model import ODKData
from mex.extractors.odk.transform import (
    assign_resource_relations,
    transform_odk_data_to_extracted_variables,
    transform_odk_resources_to_mex_resources,
)
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.wikidata.extract import (
    get_merged_organization_id_by_query_with_transform_and_load,
)


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
        extract_mapping_data(file)
        for file in Path(settings.odk.mapping_path).glob("resource_*.yaml")
    ]


@asset(group_name="odk")
def external_partner_and_publisher_by_label(
    odk_resource_mappings: list[dict[str, Any]],
    extracted_primary_source_wikidata: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract partner organizations and return their IDs grouped by query string."""
    wikidata_partner_organizations_by_query = (
        get_external_partner_and_publisher_by_label(
            transform_mapping_data_to_models(odk_resource_mappings, ExtractedResource)
        )
    )

    return get_merged_organization_id_by_query_with_transform_and_load(
        wikidata_partner_organizations_by_query, extracted_primary_source_wikidata
    )


@asset(group_name="odk")
def extracted_resources_odk(
    odk_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    external_partner_and_publisher_by_label: dict[str, MergedOrganizationIdentifier],
    extracted_primary_source_mex: ExtractedPrimarySource,
) -> list[ExtractedResource]:
    """Transform odk resources to mex resource, load to sinks and return."""
    extracted_resources_odk, is_part_of = transform_odk_resources_to_mex_resources(
        transform_mapping_data_to_models(odk_resource_mappings, ExtractedResource),
        unit_stable_target_ids_by_synonym,
        external_partner_and_publisher_by_label,
        extracted_primary_source_mex,
    )
    extracted_resources_odk_linked = assign_resource_relations(
        extracted_resources_odk, is_part_of
    )
    load(extracted_resources_odk_linked)
    return extracted_resources_odk_linked


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
def run() -> None:
    """Run the odk extractor job in-process."""
    run_job_in_process("odk")

from mex.common.cli import entrypoint
from mex.common.ldap.transform import transform_ldap_persons_to_mex_persons
from mex.common.models import (
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.mapping.transform import transform_mapping_data_to_model
from mex.extractors.open_data.extract import (
    extract_parent_resources,
    extract_resource_versions,
)
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.open_data.transform import (
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_persons,
    transform_open_data_resource_version_to_mex_resource,
)
from mex.extractors.pipeline import asset
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="open_data")
def open_data_parent_resources() -> list[OpenDataParentResource]:
    """Extract open data parent resources from Zenodo."""
    return list(extract_parent_resources())


@asset(group_name="open_data")
def open_data_resource_versions(
    open_data_parent_resources: list[OpenDataParentResource],
) -> list[OpenDataResourceVersion]:
    """Extract all versions of the parent resources from Zenodo."""
    return list(extract_resource_versions(open_data_parent_resources))


@asset(group_name="open_data", deps=["extracted_primary_source_mex"])
def extracted_primary_source_open_data(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return open data primary source and load it to sinks."""
    (extracted_primary_source_open_data,) = get_primary_sources_by_name(
        extracted_primary_sources, "open-data"
    )
    load([extracted_primary_source_open_data])
    return extracted_primary_source_open_data


@asset(group_name="open_data")
def extracted_open_data_persons(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> list[ExtractedPerson]:
    """Extract ldap persons for open data from ldap and transform them to mex persons and load them to sinks."""  # noqa: E501
    ldap_persons = transform_open_data_persons(open_data_resource_versions)
    mex_persons = list(
        transform_ldap_persons_to_mex_persons(
            ldap_persons, extracted_primary_source_ldap, extracted_organizational_units
        )
    )
    load(mex_persons)
    return mex_persons


@asset(group_name="open_data")
def extracted_open_data_parent_resources(
    open_data_parent_resources: list[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedResource]:
    """Transform open data resources to extracted resources and load them to the sinks."""  # noqa: E501
    settings = Settings.get()
    resource_mapping = transform_mapping_data_to_model(
        extract_mapping_data(settings.open_data.mapping_path / "resource.yaml"),
        ExtractedResource,
    )

    mex_sources = list(
        transform_open_data_parent_resource_to_mex_resource(
            open_data_parent_resources,
            extracted_primary_source_open_data,
            extracted_open_data_persons,
            resource_mapping,
            unit_stable_target_ids_by_synonym,
        )
    )
    load(mex_sources)
    return mex_sources


@asset(group_name="open_data")
def extracted_open_data_resource_versions(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_parent_resources: list[ExtractedResource],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedResource]:
    """Transform open data resources to extracted resources and load them to the sinks."""  # noqa: E501
    settings = Settings.get()
    resource_mapping = transform_mapping_data_to_model(
        extract_mapping_data(settings.open_data.mapping_path / "resource.yaml"),
        ExtractedResource,
    )

    mex_sources = list(
        transform_open_data_resource_version_to_mex_resource(
            open_data_resource_versions,
            extracted_primary_source_open_data,
            extracted_open_data_persons,
            extracted_open_data_parent_resources,
            resource_mapping,
            unit_stable_target_ids_by_synonym,
        )
    )
    load(mex_sources)
    return mex_sources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the odk extractor job in-process."""
    run_job_in_process("open_data")

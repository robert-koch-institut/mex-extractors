from mex.common.cli import entrypoint
from mex.common.models import (
    ConsentMapping,
    DistributionMapping,
    ExtractedConsent,
    ExtractedDistribution,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.open_data.extract import (
    extract_parent_resources,
    extract_resource_versions,
)
from mex.extractors.open_data.models.source import (
    MexPersonAndCreationDate,
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.open_data.transform import (
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_to_mex_consent,
    transform_open_data_persons,
    transform_open_data_resource_version_to_mex_resource,
)
from mex.extractors.pipeline import asset
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


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
def extracted_open_data_persons_and_creation_date(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, MexPersonAndCreationDate]:
    """Extract ldap persons and creation date for respective data sets."""
    return transform_open_data_persons(
        open_data_resource_versions,
        extracted_primary_source_ldap,
        extracted_organizational_units,
    )


@asset(group_name="open_data")
def extracted_open_data_persons(
    extracted_open_data_persons_and_creation_date: dict[str, MexPersonAndCreationDate],
) -> list[ExtractedPerson]:
    """Extract ldap persons for open data from ldap and transform them to mex persons and load them to sinks."""  # noqa: E501
    mex_persons = [
        person.mex_person
        for person in list(extracted_open_data_persons_and_creation_date.values())
    ]

    load(mex_persons)
    return mex_persons


@asset(group_name="open_data")
def extracted_open_data_distribution(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_open_data: ExtractedPrimarySource,
) -> list[ExtractedDistribution]:
    """Extract distributions for open data and transform and load them to sinks."""
    settings = Settings.get()
    distribution_mapping = DistributionMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "distribution.yaml")
    )
    mex_distributions = list(
        transform_open_data_distributions(
            open_data_resource_versions,
            extracted_primary_source_open_data,
            distribution_mapping,
        )
    )

    load(mex_distributions)
    return mex_distributions


@asset(group_name="open_data")
def extracted_open_data_parent_resources(
    open_data_parent_resources: list[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedResource]:
    """Transform open data resources to extracted resources and load them to the sinks."""  # noqa: E501
    settings = Settings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource.yaml")
    )

    mex_sources = list(
        transform_open_data_parent_resource_to_mex_resource(
            open_data_parent_resources,
            extracted_primary_source_open_data,
            extracted_primary_source_ldap,
            extracted_open_data_persons,
            unit_stable_target_ids_by_synonym,
            resource_mapping,
        )
    )
    load(mex_sources)
    return mex_sources


@asset(group_name="open_data")
def extracted_open_data_resource_versions(  # noqa: PLR0913
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_parent_resources: list[ExtractedResource],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_open_data_distribution: list[ExtractedDistribution],
) -> list[ExtractedResource]:
    """Transform open data resources to extracted resources and load them to the sinks."""  # noqa: E501
    settings = Settings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource.yaml")
    )

    mex_sources = list(
        transform_open_data_resource_version_to_mex_resource(
            open_data_resource_versions,
            extracted_primary_source_open_data,
            extracted_primary_source_ldap,
            extracted_open_data_persons,
            extracted_open_data_parent_resources,
            unit_stable_target_ids_by_synonym,
            extracted_open_data_distribution,
            resource_mapping,
        )
    )
    load(mex_sources)
    return mex_sources


@asset(group_name="open_data")
def extracted_open_data_consent(
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_persons_and_creation_date: dict[str, MexPersonAndCreationDate],
) -> list[ExtractedConsent]:
    """Transform open data persons to extracted consents and load them to the sinks."""
    settings = Settings.get()
    consent_mapping = ConsentMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "consent.yaml")
    )

    mex_consents = list(
        transform_open_data_person_to_mex_consent(
            extracted_primary_source_open_data,
            extracted_open_data_persons,
            extracted_open_data_persons_and_creation_date,
            consent_mapping,
        )
    )
    load(mex_consents)
    return mex_consents


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the odk extractor job in-process."""
    run_job_in_process("open_data")

from dagster import asset
from mex.common.cli import entrypoint
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import transform_ldap_actor_to_mex_contact_point
from mex.common.models import (
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.open_data.extract import (
    extract_open_data_persons_from_open_data_parent_resources,
    extract_parent_resources,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
)
from mex.extractors.open_data.transform import (
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_affiliations_to_organizations,
    transform_open_data_persons,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


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
def open_data_parent_resources() -> list[OpenDataParentResource]:
    """Extract open data parent resources from Zenodo."""
    return extract_parent_resources()


@asset(group_name="open_data")
def extracted_open_data_creators_contributors(
    open_data_parent_resources: list[OpenDataParentResource],
) -> list[OpenDataCreatorsOrContributors]:
    """Return unique open Data persons from open data parent resources."""
    return extract_open_data_persons_from_open_data_parent_resources(
        open_data_parent_resources
    )


@asset(group_name="open_data")
def extracted_open_data_organizations(
    extracted_open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    extracted_primary_source_open_data: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Transform affiliations of open data persons to extracted organizations."""
    return transform_open_data_person_affiliations_to_organizations(
        extracted_open_data_creators_contributors, extracted_primary_source_open_data
    )


@asset(group_name="open_data")
def extracted_open_data_persons(  # noqa: PLR0913
    extracted_open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
    extracted_open_data_organizations: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedPerson]:
    """Get Extracted persons and load them to sinks."""
    extracted_open_data_persons = transform_open_data_persons(
        extracted_open_data_creators_contributors,
        extracted_primary_source_ldap,
        extracted_primary_source_open_data,
        extracted_organizational_units,
        extracted_organization_rki,
        extracted_open_data_organizations,
    )
    load(extracted_open_data_persons)
    return extracted_open_data_persons


@asset(group_name="open_data")
def open_data_contact_point(
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> list[ExtractedContactPoint]:
    """Convert open data email address to contact point and load to sink."""
    ldap = LDAPConnector.get()
    contact_point = [
        transform_ldap_actor_to_mex_contact_point(
            ldap.get_functional_account(mail="opendata@rki.de"),
            extracted_primary_source_ldap,
        )
    ]

    load(contact_point)
    return contact_point


@asset(group_name="open_data")
def extracted_open_data_distribution(
    open_data_parent_resources: list[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
) -> list[ExtractedDistribution]:
    """Extract distributions for open data & transform and load them to sinks."""
    settings = Settings.get()
    distribution_mapping = DistributionMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "distribution.yaml")
    )
    mex_distributions = transform_open_data_distributions(
        open_data_parent_resources,
        extracted_primary_source_open_data,
        distribution_mapping,
    )

    load(mex_distributions)
    return mex_distributions


@asset(group_name="open_data")
def extracted_open_data_parent_resources(  # noqa: PLR0913
    open_data_parent_resources: list[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_open_data_distribution: list[ExtractedDistribution],
    extracted_organization_rki: ExtractedOrganization,
    open_data_contact_point: list[ExtractedContactPoint],
) -> list[ExtractedResource]:
    """Transform parent resources to extracted resources & load them to the sinks."""
    settings = Settings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource.yaml")
    )

    mex_sources = transform_open_data_parent_resource_to_mex_resource(
        open_data_parent_resources,
        extracted_primary_source_open_data,
        extracted_open_data_persons,
        unit_stable_target_ids_by_synonym,
        extracted_open_data_distribution,
        resource_mapping,
        extracted_organization_rki,
        open_data_contact_point,
    )

    load(mex_sources)
    return mex_sources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the odk extractor job in-process."""
    run_job_in_process("open_data")

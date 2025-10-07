from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.transform import transform_ldap_persons_to_extracted_persons
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.biospecimen.extract import (
    extract_biospecimen_contacts_by_email,
    extract_biospecimen_organizations,
    extract_biospecimen_resources,
)
from mex.extractors.biospecimen.models.source import BiospecimenResource
from mex.extractors.biospecimen.transform import (
    transform_biospecimen_resource_to_mex_resource,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@asset(group_name="biospecimen", deps=["extracted_primary_source_mex"])
def biospecimen_extracted_primary_source(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return biospecimen primary source and load them to sinks."""
    (biospecimen_extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "biospecimen"
    )
    load([biospecimen_extracted_primary_source])
    return biospecimen_extracted_primary_source


@asset(group_name="biospecimen")
def biospecimen_resources() -> list[BiospecimenResource]:
    """Extract from biospecimen resources."""
    return list(extract_biospecimen_resources())


@asset(group_name="biospecimen")
def biospecimen_extracted_mex_persons(
    biospecimen_resources: list[BiospecimenResource],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedPerson]:
    """Extract ldap persons for biospecimen from ldap and transform them to mex persons and load them to sinks."""  # noqa: E501
    ldap_persons = extract_biospecimen_contacts_by_email(biospecimen_resources)
    mex_persons = transform_ldap_persons_to_extracted_persons(
        ldap_persons,
        extracted_primary_source_ldap,
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_persons)
    return mex_persons


@asset(group_name="biospecimen")
def biospecimen_extracted_resources(  # noqa: PLR0913
    biospecimen_resources: list[BiospecimenResource],
    biospecimen_extracted_primary_source: ExtractedPrimarySource,
    biospecimen_extracted_mex_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    synopse_activities: list[ExtractedActivity],
) -> list[ExtractedResource]:
    """Transform biospecimen resources to extracted resources and load them to the sinks."""  # noqa: E501
    settings = Settings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(settings.biospecimen.mapping_path / "resource.yaml")
    )
    extracted_organizations = extract_biospecimen_organizations(biospecimen_resources)

    mex_sources = list(
        transform_biospecimen_resource_to_mex_resource(
            biospecimen_resources,
            biospecimen_extracted_primary_source,
            unit_stable_target_ids_by_synonym,
            biospecimen_extracted_mex_persons,
            extracted_organization_rki,
            synopse_activities,
            resource_mapping,
            extracted_organizations,
        )
    )
    load(mex_sources)
    return mex_sources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the biospecimen extractor job in-process."""
    run_job_in_process("biospecimen")

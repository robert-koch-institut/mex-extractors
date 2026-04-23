from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import MergedPersonIdentifier
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


@asset(group_name="biospecimen")
def biospecimen_resources() -> list[BiospecimenResource]:
    """Extract from biospecimen resources."""
    return extract_biospecimen_resources()


@asset(group_name="biospecimen")
def biospecimen_extracted_persons(
    biospecimen_resources: list[BiospecimenResource],
) -> dict[str, MergedPersonIdentifier]:
    """Extract ldap persons for biospecimen from ldap and transform them to mex persons and load them to sinks."""  # noqa: E501
    return extract_biospecimen_contacts_by_email(biospecimen_resources)


@asset(group_name="biospecimen")
def biospecimen_extracted_resources(
    biospecimen_resources: list[BiospecimenResource],
    biospecimen_extracted_persons: dict[str, MergedPersonIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    synopse_extracted_activities: list[ExtractedActivity],
) -> list[ExtractedResource]:
    """Transform biospecimen resources to extracted resources and load them to the sinks."""  # noqa: E501
    settings = Settings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(settings.biospecimen.mapping_path / "resource.yaml")
    )
    extracted_organizations = extract_biospecimen_organizations(biospecimen_resources)

    mex_sources = transform_biospecimen_resource_to_mex_resource(
        biospecimen_resources,
        biospecimen_extracted_persons,
        extracted_organization_rki,
        synopse_extracted_activities,
        resource_mapping,
        extracted_organizations,
    )
    load(mex_sources)
    return mex_sources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the biospecimen extractor job in-process."""
    run_job_in_process("biospecimen")

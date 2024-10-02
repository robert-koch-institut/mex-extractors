from mex.common.cli import entrypoint
from mex.common.ldap.transform import transform_ldap_persons_to_mex_persons
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
)
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.rdmo.extract import (
    extract_rdmo_source_contacts,
    extract_rdmo_sources,
)
from mex.extractors.rdmo.models.source import RDMOSource
from mex.extractors.rdmo.transform import transform_rdmo_sources_to_extracted_activities
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="rdmo", deps=["extracted_primary_source_mex"])
def extracted_primary_source_rdmo(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return RDMO extracted primary source."""
    (extracted_primary_source_rdmo,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "rdmo",
    )
    load([extracted_primary_source_rdmo])
    return extracted_primary_source_rdmo


@asset(group_name="rdmo")
def rdmo_sources() -> list[RDMOSource]:
    """Extract project sources from RDMO."""
    return list(extract_rdmo_sources())


@asset(group_name="rdmo")
def rdmo_contacts(
    rdmo_sources: list[RDMOSource],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> list[ExtractedPerson]:
    """Extract contacts for RDMO sources and load them."""
    ldap_persons = extract_rdmo_source_contacts(rdmo_sources)
    extracted_persons = list(
        transform_ldap_persons_to_mex_persons(
            ldap_persons,
            extracted_primary_source_ldap,
            extracted_organizational_units,
        )
    )
    load(extracted_persons)
    return extracted_persons


@asset(group_name="rdmo", deps=["rdmo_contacts"])
def rdmo_activities(
    rdmo_sources: list[RDMOSource],
    extracted_primary_source_rdmo: ExtractedPrimarySource,
) -> list[ExtractedActivity]:
    """Transform RDMO project sources to activities and load them."""
    extracted_activities = list(
        transform_rdmo_sources_to_extracted_activities(
            rdmo_sources, extracted_primary_source_rdmo
        )
    )
    load(extracted_activities)
    return extracted_activities


@entrypoint(Settings)
def run() -> None:
    """Run the RDMO extractor job in-process."""
    run_job_in_process("rdmo")

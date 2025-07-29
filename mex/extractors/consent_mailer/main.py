from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import ExtractedPrimarySource, MergedPerson
from mex.extractors.consent_mailer.extract import (
    extract_consents_for_persons,
    extract_ldap_persons,
)
from mex.extractors.consent_mailer.filter import filter_persons_without_consent
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="consent_mailer")
def all_ldap_persons_for_mailer(
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> list[MergedPerson]:
    """Get LDAP persons without consent for mailer ."""
    ldap_merged_persons = extract_ldap_persons(
        extracted_primary_source_ldap.stableTargetId
    )
    merged_consents = extract_consents_for_persons(ldap_merged_persons)
    return filter_persons_without_consent(ldap_merged_persons, merged_consents)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the consent mailer job in-process."""
    run_job_in_process("consent_mailer")

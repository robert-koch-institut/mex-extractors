import smtplib

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import ExtractedPrimarySource, MergedPerson
from mex.extractors.consent_mailer.extract import (
    extract_consents_for_persons,
    extract_ldap_persons,
)
from mex.extractors.consent_mailer.filter import (
    filter_persons_without_consent,
)
from mex.extractors.consent_mailer.transform import transform_person_to_sendable_email
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="consent_mailer")
def consent_mailer_merged_persons_without_consent(
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> list[MergedPerson]:
    """Get LDAP persons without consent for mailer ."""
    ldap_merged_persons = extract_ldap_persons(
        extracted_primary_source_ldap.stableTargetId
    )
    merged_consents = extract_consents_for_persons(ldap_merged_persons)
    return filter_persons_without_consent(ldap_merged_persons, merged_consents)


@asset(group_name="consent_mailer")
def consent_mailer_send_emails(
    consent_mailer_merged_persons_without_consent: list[MergedPerson],
) -> None:
    """Send consent emails to the given persons.

    Args:
        consent_mailer_merged_persons_without_consent: The list of persons that gets
        the consent email.
    """
    settings = Settings.get()
    host, port = settings.consent_mailer.smtp_server.split(":")

    mails_for_persons = [
        transform_person_to_sendable_email(person)
        for person in consent_mailer_merged_persons_without_consent
    ]

    with smtplib.SMTP(host, int(port)) as s:
        for mail in mails_for_persons:
            if mail:
                s.send_message(mail)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the consent mailer job in-process."""
    run_job_in_process("consent_mailer")

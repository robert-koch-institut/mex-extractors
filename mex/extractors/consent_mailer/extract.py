from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import MergedConsent, MergedPerson
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.settings import Settings


def extract_ldap_persons(
    extracted_primary_source_ldap_identifier: MergedPrimarySourceIdentifier,
) -> list[MergedPerson]:
    """Get all persons from primary source LDAP."""
    connector = BackendApiConnector.get()
    return cast(
        "list[MergedPerson]",
        list(
            connector.fetch_all_merged_items(
                entity_type=["MergedPerson"],
                reference_field="hadPrimarySource",
                referenced_identifier=[extracted_primary_source_ldap_identifier],
            )
        ),
    )


def extract_consents_for_persons(
    person_items: list[MergedPerson],
) -> list[MergedConsent]:
    """Get consents for ldap persons."""
    settings = Settings.get()
    connector = BackendApiConnector.get()
    person_ids = [str(person.identifier) for person in person_items]

    if not person_ids:
        return []

    chunk_size = settings.consent_mailer.backend_fetch_chunk_size
    collected_merged_consents: list[MergedConsent] = []
    for i in range(0, len(person_ids), chunk_size):
        partial_person_ids = person_ids[i : i + chunk_size]
        partial_merged_consents = cast(
            "list[MergedConsent]",
            list(
                connector.fetch_all_merged_items(
                    entity_type=["MergedConsent"],
                    reference_field="hasDataSubject" if partial_person_ids else None,
                    referenced_identifier=partial_person_ids,
                )
            ),
        )
        collected_merged_consents.extend(partial_merged_consents)
    return collected_merged_consents

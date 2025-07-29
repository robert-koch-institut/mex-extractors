from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import MergedConsent, MergedPerson
from mex.common.types import MergedPrimarySourceIdentifier


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
    connector = BackendApiConnector.get()
    person_ids = [str(person.identifier) for person in person_items]
    return cast(
        "list[MergedConsent]",
        list(
            connector.fetch_all_merged_items(
                entity_type=["MergedConsent"],
                reference_field="hasDataSubject",
                referenced_identifier=person_ids,
            )
        ),
    )

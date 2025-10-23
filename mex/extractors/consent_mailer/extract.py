from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import MergedConsent, MergedPerson
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


def extract_ldap_persons() -> list[MergedPerson]:
    """Get all persons from primary source LDAP."""
    connector = BackendApiConnector.get()
    return cast(
        "list[MergedPerson]",
        list(
            connector.fetch_all_merged_items(
                entity_type=["MergedPerson"],
                reference_field="hadPrimarySource",
                referenced_identifier=[get_extracted_primary_source_id_by_name("ldap")],
            )
        ),
    )


def extract_consents_for_persons(
    person_items: list[MergedPerson],
) -> list[MergedConsent]:
    """Get consents for ldap persons."""
    connector = BackendApiConnector.get()
    person_ids = [str(person.identifier) for person in person_items]

    if not person_ids:
        return []

    return cast(
        "list[MergedConsent]",
        list(
            connector.fetch_all_merged_items(
                entity_type=["MergedConsent"],
                reference_field="hasDataSubject" if person_ids else None,
                referenced_identifier=person_ids,
            )
        ),
    )

from typing import TYPE_CHECKING, cast

from mex.common.backend_api.connector import BackendApiConnector, ReferenceFilter
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import ExtractorsSettings

if TYPE_CHECKING:
    from mex.common.models import MergedConsent, MergedPerson


def extract_consent_mailer_ldap_persons() -> list[MergedPerson]:
    """Get all persons from primary source LDAP."""
    connector = BackendApiConnector.get()
    return cast(
        "list[MergedPerson]",
        list(
            connector.fetch_all_publishable_merged_items(
                publishing_target="invenio",
                entity_type=["MergedPerson"],
                reference_filters=[
                    ReferenceFilter(
                        field="hadPrimarySource",
                        identifiers=[get_extracted_primary_source_id_by_name("ldap")],
                    )
                ],
            )
        ),
    )


def extract_consents_for_persons(
    person_items: list[MergedPerson],
) -> list[MergedConsent]:
    """Get consents for ldap persons."""
    settings = ExtractorsSettings.get()
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
                connector.fetch_all_publishable_merged_items(
                    publishing_target="invenio",
                    entity_type=["MergedConsent"],
                    reference_filters=[
                        ReferenceFilter(
                            field="hasDataSubject", identifiers=partial_person_ids
                        )
                    ]
                    if partial_person_ids
                    else None,
                )
            ),
        )
        collected_merged_consents.extend(partial_merged_consents)
    return collected_merged_consents

from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.common.models import MergedConsent, MergedPerson

if TYPE_CHECKING:
    from mex.common.types import MergedPersonIdentifier


def filter_persons_with_consent(
    person_items: list[MergedPerson], consent_items: list[MergedConsent]
) -> list[MergedPerson]:
    """Filter for persons with positive consent. Fail if a person has > 1 consent.

    Args:
        person_items: list of persons
        consent_items: list of consents

    Returns:
        list of filtered persons with positive consent.
        raises an error if any person has more than one consent
    """
    person_items_by_id = {person.identifier: person for person in person_items}

    seen_person_ids_with_any_consent: set[MergedPersonIdentifier] = set()
    collected_person_items_with_positive_consent: list[MergedPerson] = []

    for consent in consent_items:
        person_id = consent.hasDataSubject

        if person_id in seen_person_ids_with_any_consent:
            msg = f"Merged Person {person_id} has more than 1 consent."
            raise MExError(msg)

        seen_person_ids_with_any_consent.add(person_id)

        if (
            consent.hasConsentStatus.name == "VALID_FOR_PROCESSING"
            and person_id in person_items_by_id
        ):
            collected_person_items_with_positive_consent.append(
                person_items_by_id[person_id]
            )

    return collected_person_items_with_positive_consent

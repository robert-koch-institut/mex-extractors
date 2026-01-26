from collections import defaultdict
from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.common.models import MergedConsent, MergedPerson

if TYPE_CHECKING:
    from mex.common.types import MergedConsentIdentifier, MergedPersonIdentifier


def filter_persons_with_a_unique_consent(
    person_items: list[MergedPerson], consent_items: list[MergedConsent]
) -> list[MergedPerson]:
    """Filter for persons with positive consent. Fail if a person has > 1 consent.

    Args:
        person_items: list of persons
        consent_items: list of consents

    Returns:
        list of filtered persons with positive consent.
        raises an error if any person has more than one consent. log all concerned
                persons and consents
    """
    person_items_by_id = {person.identifier: person for person in person_items}

    seen_person_ids_with_consent_ids: dict[
        MergedPersonIdentifier, list[MergedConsentIdentifier]
    ] = defaultdict(list)
    collected_person_items_with_positive_consent: list[MergedPerson] = []

    for consent in consent_items:
        person_id = consent.hasDataSubject
        seen_person_ids_with_consent_ids[person_id].append(consent.identifier)

        if (
            consent.hasConsentStatus.name == "VALID_FOR_PROCESSING"
            and (person := person_items_by_id.get(person_id)) is not None
        ):
            collected_person_items_with_positive_consent.append(person)

    persons_with_serveral_consents = {
        p: c for p, c in seen_person_ids_with_consent_ids.items() if len(c) > 1
    }
    if persons_with_serveral_consents:
        msg = (
            f"The following Merged Persons are referenced by more than one "
            f"Merged Consent: {persons_with_serveral_consents}."
        )
        raise MExError(msg)

    return collected_person_items_with_positive_consent

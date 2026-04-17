from collections import defaultdict
from typing import TYPE_CHECKING

from mex.common.exceptions import MExError

if TYPE_CHECKING:
    from mex.common.models import MergedConsent, MergedPerson
    from mex.common.types import MergedConsentIdentifier, MergedPersonIdentifier


def filter_persons_with_approving_unique_consent(
    person_items: list[MergedPerson], consent_items: list[MergedConsent]
) -> list[MergedPerson]:
    """Filter for persons with approving consent. Fail if a person has > 1 consent.

    Args:
        person_items: list of persons
        consent_items: list of consents

    Raises:
        MExError if any person has more than one consent. The error lists all concerned
                persons and consents.

    Returns:
        list of filtered persons with approving consent.
    """
    person_items_by_id = {person.identifier: person for person in person_items}

    seen_person_ids_with_consent_ids: dict[
        MergedPersonIdentifier, list[MergedConsentIdentifier]
    ] = defaultdict(list)
    collected_persons_with_positive_consent: list[MergedPerson] = []

    for consent in consent_items:
        person_id = consent.hasDataSubject
        seen_person_ids_with_consent_ids[person_id].append(consent.identifier)

        if (
            consent.hasConsentStatus.name == "VALID_FOR_PROCESSING"
            and (person := person_items_by_id.get(person_id)) is not None
        ):
            collected_persons_with_positive_consent.append(person)

    persons_with_serveral_consents = {
        p: c for p, c in seen_person_ids_with_consent_ids.items() if len(c) > 1
    }
    if persons_with_serveral_consents:
        msg = (
            f"The following Merged Persons are referenced by more than one "
            f"Merged Consent: {persons_with_serveral_consents}."
        )
        raise MExError(msg)

    return collected_persons_with_positive_consent

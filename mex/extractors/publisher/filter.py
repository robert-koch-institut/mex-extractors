from mex.common.models import MergedConsent, MergedPerson


def filter_persons_with_consent(
    person_items: list[MergedPerson], consent_items: list[MergedConsent]
) -> list[MergedPerson]:
    """Filter person items for having consent.

    Args:
        person_items: list of persons
        consent_items: list of consents

    Returns:
        list of filtered persons without consent.
    """
    person_ids_with_positive_consent = [
        consent.hasDataSubject
        for consent in consent_items
        if consent.hasConsentStatus.name == "VALID_FOR_PROCESSING"
    ]
    return [
        person
        for person in person_items
        if person.identifier in person_ids_with_positive_consent
    ]

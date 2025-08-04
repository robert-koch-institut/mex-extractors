from mex.common.models import MergedConsent, MergedPerson


def filter_persons_without_consent(
    person_items: list[MergedPerson], consent_items: list[MergedConsent]
) -> list[MergedPerson]:
    """Filter person items for having no consent.

    Args:
        person_items: list of persons
        consent_items: list of consents

    Returns:
        list of filtered persons without consent.
    """
    person_ids_with_consent = [consent.hasDataSubject for consent in consent_items]
    return [
        person
        for person in person_items
        if person.identifier not in person_ids_with_consent
    ]

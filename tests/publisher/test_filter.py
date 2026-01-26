import re

import pytest

from mex.common.exceptions import MExError
from mex.common.models import MergedConsent, MergedPerson
from mex.extractors.publisher.filter import (
    filter_persons_with_appoving_unique_consent,
)


def test_filter_persons_with_appoving_unique_consent(
    merged_person_list: list[MergedPerson],
    merged_consent_list: list[MergedConsent],
) -> None:
    result = filter_persons_with_appoving_unique_consent(
        merged_person_list,
        merged_consent_list[0:1],  # only consents referencing different persons
    )
    assert len(result) == 1
    assert result[0].model_dump(exclude_defaults=True, mode="json") == {
        "identifier": "PersonPositiveConsent",
        "fullName": ["Person, with positive Consent"],
        "memberOf": ["SomeUnitIdentifier"],
    }


def test_filter_persons_with_appoving_unique_consent__raise(
    merged_person_list: list[MergedPerson],
    merged_consent_list: list[MergedConsent],
) -> None:
    expected = (
        "MExError: The following Merged Persons are referenced by more than one "
        'Merged Consent: {MergedPersonIdentifier("PersonHasTwoConsents"): '
        '[MergedConsentIdentifier("Consent1SamePerson"),'
        ' MergedConsentIdentifier("Consent2SamePerson")]}.'
    )
    with pytest.raises(MExError, match=re.escape(expected)):
        filter_persons_with_appoving_unique_consent(
            merged_person_list,
            merged_consent_list,  # all consents incl. those referencing the same person
        )

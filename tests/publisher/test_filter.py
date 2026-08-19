import re
from typing import TYPE_CHECKING

import pytest

from mex.common.exceptions import MExError
from mex.common.testing import Joker
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.publisher.filter import (
    cluster_and_filter_bibliographic_resources_by_unit,
    filter_persons_with_approving_unique_consent,
)

if TYPE_CHECKING:
    from mex.common.models import (
        MergedBibliographicResource,
        MergedConsent,
        MergedPerson,
    )


def test_filter_persons_with_approving_unique_consent(
    merged_person_list: list[MergedPerson],
    merged_consent_list: list[MergedConsent],
) -> None:
    result = filter_persons_with_approving_unique_consent(
        merged_person_list,
        merged_consent_list[0:1],  # only consents referencing different persons
    )
    assert len(result) == 1
    assert result[0].model_dump(exclude_defaults=True, mode="json") == {
        "identifier": "PersonPositiveConsent",
        "fullName": ["Person, with positive Consent"],
        "memberOf": ["SomeUnitIdentifier"],
    }


def test_filter_persons_with_approving_unique_consent__raise(
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
        filter_persons_with_approving_unique_consent(
            merged_person_list,
            merged_consent_list,  # all consents incl. those referencing the same person
        )


@pytest.mark.usefixtures("mocked_backend", "mocked_wikidata")
def test_cluster_and_filter_bibliographic_resources_by_unit(
    merged_bibliographic_resource_list: list[MergedBibliographicResource],
) -> None:
    publication_by_department = cluster_and_filter_bibliographic_resources_by_unit(
        merged_bibliographic_resource_list
    )
    assert publication_by_department.keys() == {"hIiJpZXVppHvoyeP0QtAoS"}

    assert publication_by_department[
        MergedOrganizationalUnitIdentifier("hIiJpZXVppHvoyeP0QtAoS")
    ][0].model_dump(exclude_defaults=True, mode="json") == {
        "accessRestriction": Joker(),
        "creator": ["PersonIdentifier"],
        "title": [{"value": "title 1, Unit Parent"}],
        "contributingUnit": ["hIiJpZXVppHvoyeP0QtAoS"],
        "identifier": "PublicationOfPRNTUnit",
    }

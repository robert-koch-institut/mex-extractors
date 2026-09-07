import re
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from mex.common.exceptions import MExError
from mex.common.testing import Joker
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.publisher.filter import (
    _cluster_publications_by_department,
    _get_forbidden_units,
    cluster_and_filter_bibliographic_resources_by_unit,
    filter_persons_with_approving_unique_consent,
)
from mex.extractors.settings import ExtractorsSettings

if TYPE_CHECKING:
    from mex.common.models import (
        MergedBibliographicResource,
        MergedConsent,
        MergedOrganizationalUnit,
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


def test_get_forbidden_units(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = ExtractorsSettings.get()

    unit_ids_by_synonym = {
        "fg99": [MergedOrganizationalUnitIdentifier("cjna2jitPngp6yIV63cdi9")],
    }
    mocked_get_unit_merged_id_by_synonym = MagicMock(
        side_effect=unit_ids_by_synonym.get
    )
    monkeypatch.setattr(
        "mex.extractors.publisher.filter.get_unit_merged_id_by_synonym",
        mocked_get_unit_merged_id_by_synonym,
    )

    result = _get_forbidden_units(settings)

    assert result == {
        MergedOrganizationalUnitIdentifier("cjna2jitPngp6yIV63cdi9"),
    }
    mocked_get_unit_merged_id_by_synonym.assert_any_call("fg99")


def test_cluster_publications_by_department(
    monkeypatch: pytest.MonkeyPatch,
    merged_bibliographic_resource_list: list[MergedBibliographicResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    department_unit_id = MergedOrganizationalUnitIdentifier("hIiJpZXVppHvoyeP0QtAoS")
    child_unit_id = MergedOrganizationalUnitIdentifier("6rqNvZSApUHlz8GkkVP48")
    forbidden_unit_id = MergedOrganizationalUnitIdentifier("cjna2jitPngp6yIV63cdi9")

    mocked_find_descendants = MagicMock(return_value=[str(child_unit_id)])

    monkeypatch.setattr(
        "mex.extractors.publisher.filter.find_descendants",
        mocked_find_descendants,
    )

    result = _cluster_publications_by_department(
        department_unit_id,
        mocked_merged_organizational_units,
        {forbidden_unit_id},
        merged_bibliographic_resource_list,
    )

    assert [str(publication.identifier) for publication in result] == [
        "PublicationOfC1",
        "PublicationOfPRNTUnit",
    ]

    mocked_find_descendants.assert_called_once_with(
        mocked_merged_organizational_units,
        str(department_unit_id),
    )


@pytest.mark.usefixtures("mocked_backend_publisher", "mocked_wikidata")
def test_cluster_and_filter_bibliographic_resources_by_unit(
    monkeypatch: pytest.MonkeyPatch,
    merged_bibliographic_resource_list: list[MergedBibliographicResource],
) -> None:
    mocked_find_descendants = MagicMock(return_value=[])

    monkeypatch.setattr(
        "mex.extractors.publisher.filter.find_descendants",
        mocked_find_descendants,
    )

    result = cluster_and_filter_bibliographic_resources_by_unit(
        [merged_bibliographic_resource_list[1]]
    )

    assert mocked_find_descendants.call_count == 2
    assert result.keys() == {"hIiJpZXVppHvoyeP0QtAoS", "cjna2jitPngp6yIV63cdi9"}
    assert (
        len(result[MergedOrganizationalUnitIdentifier("hIiJpZXVppHvoyeP0QtAoS")]) == 1
    )
    assert (
        len(result[MergedOrganizationalUnitIdentifier("cjna2jitPngp6yIV63cdi9")]) == 0
    )
    assert result[MergedOrganizationalUnitIdentifier("hIiJpZXVppHvoyeP0QtAoS")][
        0
    ].model_dump(exclude_defaults=True, mode="json") == {
        "accessRestriction": Joker(),
        "contributingUnit": ["hIiJpZXVppHvoyeP0QtAoS"],
        "creator": ["PersonIdentifier"],
        "identifier": "PublicationOfPRNTUnit",
        "publicationYear": "2042",
        "title": [{"value": "title 1, Unit Parent"}],
    }

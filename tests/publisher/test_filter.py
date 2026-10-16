import re
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from mex.common.exceptions import MExError
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
        ExtractedOrganizationalUnit,
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
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    department_unit_id = mocked_units_by_identifier_in_primary_source[
        "parent-unit"
    ].stableTargetId
    child_unit_id = mocked_units_by_identifier_in_primary_source[
        "child-unit"
    ].stableTargetId
    forbidden_unit_id = mocked_units_by_identifier_in_primary_source[
        "fg99"
    ].stableTargetId

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
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
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
    assert result.keys() == {
        mocked_units_by_identifier_in_primary_source["parent-unit"].stableTargetId,
        mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
    }
    assert (
        len(
            result[
                mocked_units_by_identifier_in_primary_source[
                    "parent-unit"
                ].stableTargetId
            ]
        )
        == 1
    )
    assert (
        len(result[mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId])
        == 0
    )
    assert result[
        mocked_units_by_identifier_in_primary_source["parent-unit"].stableTargetId
    ][0].model_dump(exclude_defaults=True, mode="json") == {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["parent-unit"].stableTargetId
        ],
        "creator": ["PersonIdentifier"],
        "identifier": "PublicationOfPRNTUnit",
        "publicationYear": "2042",
        "title": [{"value": "title 1, Unit Parent"}],
    }

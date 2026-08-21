from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector, ReferenceFilter
from mex.common.models import (
    AnyMergedModel,
    ItemsContainer,
    MergedBibliographicResource,
    MergedConsent,
    MergedContactPoint,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedPrimarySource,
    VersionStatus,
)
from mex.common.types import (
    AccessRestriction,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
    YearMonthDayTime,
)


@pytest.fixture
def merged_ldap_person_list() -> list[MergedPerson]:
    return [
        MergedPerson(
            identifier="PersonWithFallbackUnit",
            memberOf=["ValidUnitWithEmail", "InvalidUnitNoEmail"],
        ),
        MergedPerson(
            identifier="PersonWithoutFallback",
            memberOf=[],
        ),
    ]


@pytest.fixture
def merged_person_list() -> list[MergedPerson]:
    return [
        MergedPerson(
            identifier="PersonPositiveConsent",
            fullName=["Person, with positive Consent"],
            memberOf=["SomeUnitIdentifier"],
        ),
        MergedPerson(
            identifier="PersonNegativeConsent",
            fullName=["Person, with negative Consent"],
        ),
        MergedPerson(
            identifier="PersonHasTwoConsents",
            fullName=["Peron, with more than one Consent"],
            memberOf=["SomeUnitIdentifier"],
        ),
        MergedPerson(
            identifier="PersonNoConsentLink",
        ),
    ]


@pytest.fixture
def merged_consent_list() -> list[MergedConsent]:
    return [
        MergedConsent(
            identifier="PositiveConsent",
            hasDataSubject="PersonPositiveConsent",
            hasConsentStatus="https://mex.rki.de/item/consent-status-2",
            isIndicatedAtTime=YearMonthDayTime("1999-12-31T23:59:59Z"),
        ),
        MergedConsent(
            identifier="NegativeConsent",
            hasDataSubject="PersonNegativeConsent",
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
            isIndicatedAtTime=YearMonthDayTime("2000-01-01T00:00:00Z"),
        ),
        MergedConsent(
            identifier="Consent1SamePerson",
            hasDataSubject="PersonHasTwoConsents",
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
            isIndicatedAtTime=YearMonthDayTime("1970-01-01T00:00:00Z"),
        ),
        MergedConsent(
            identifier="Consent2SamePerson",
            hasDataSubject="PersonHasTwoConsents",
            hasConsentStatus="https://mex.rki.de/item/consent-status-2",
            isIndicatedAtTime=YearMonthDayTime("2025-12-31T23:59:59Z"),
        ),
    ]


@pytest.fixture
def merged_unit_contactpoint_container() -> ItemsContainer[AnyMergedModel]:
    return ItemsContainer[AnyMergedModel](
        items=[
            MergedOrganizationalUnit(
                identifier="ValidUnitWithEmail",
                name="unit with email",
                email=["unit@e.mail"],
            ),
            MergedOrganizationalUnit(
                identifier="InvalidUnitNoEmail",
                name="unit without email",
                email=[],
            ),
            MergedContactPoint(
                # even if they have an email address, contact points should not
                # be added to publisher_fallback_unit_identifiers_by_person,
                identifier="CPShouldBeIgnored",
                email=["contactpoint@e.mail"],
            ),
        ]
    )


@pytest.fixture
def merged_bibliographic_resource_list() -> list[MergedBibliographicResource]:
    """Mock a list of Merged Bibliographic Resource items."""
    return [
        MergedBibliographicResource(
            accessRestriction=AccessRestriction["OPEN"],
            creator=["PersonIdentifier"],
            identifier="PublicationOfC1",
            title=[Text(value="title 1, Unit C1", language=None)],
            contributingUnit=["6rqNvZSApUHlz8GkkVP48"],  # C1
        ),
        MergedBibliographicResource(
            accessRestriction=AccessRestriction["OPEN"],
            creator=["PersonIdentifier"],
            identifier="PublicationOfPRNTUnit",
            publicationYear="2042",
            title=[Text(value="title 1, Unit Parent", language=None)],
            contributingUnit=["hIiJpZXVppHvoyeP0QtAoS"],  # PRNT
        ),
        MergedBibliographicResource(
            accessRestriction=AccessRestriction["OPEN"],
            creator=["PersonIdentifier"],
            identifier="PublicationOfFG99",
            title=[Text(value="title 1, Unit FG99", language=None)],
            contributingUnit=["cjna2jitPngp6yIV63cdi9"],  # FG99
        ),
    ]


@pytest.fixture
def mocked_publisher_fallback_unit_identifiers_by_person() -> dict[
    MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]
]:
    return {
        MergedPersonIdentifier("PersonWithFallbackUnit"): [
            MergedOrganizationalUnitIdentifier("ValidUnitWithEmail")
        ]
    }


def fetch_all_publishable_merged_items(
    *,
    publishing_target: str = "target",  # noqa: ARG001
    query_string: str | None = None,  # noqa: ARG001
    identifier: str | None = None,  # noqa: ARG001
    entity_type: list[str] | None = None,
    reference_filters: list[ReferenceFilter] | None = None,  # noqa: ARG001
) -> list[AnyMergedModel]:
    merged_items: list[AnyMergedModel] = [
        MergedPrimarySource(
            identifier="fakeFakeSource",
        ),
        MergedContactPoint(
            email=["mex@rki.de"],
            identifier="fakeFakeContact",
        ),
        MergedConsent(
            identifier="fakePositiveConsent",
            hasConsentStatus="https://mex.rki.de/item/consent-status-2",
            hasDataSubject="fakeFakePerson",
            isIndicatedAtTime="2014-05-21T19:38:51Z",
        ),
        MergedOrganizationalUnit(
            email=["unit@rki.de"],
            identifier="fakeFakeOrgUnit",
            name="Unique Unit",
        ),
        MergedPerson(
            fullName="Dr. Fake",
            identifier="fakeFakePerson",
            memberOf="fakeFakeOrgUnit",
        ),
        MergedBibliographicResource(
            identifier="fakeFakeBibRes",
            accessRestriction=AccessRestriction["OPEN"],
            title="Bib 98765",
            creator=["fakeFakePerson"],
        ),
    ]
    return [
        item
        for item in merged_items
        if not entity_type or item.entityType in entity_type
    ]


@pytest.fixture
def mocked_backend_publisher(monkeypatch: MonkeyPatch) -> MagicMock:
    backend = MagicMock(
        fetch_all_publishable_merged_items=MagicMock(
            spec=BackendApiConnector.fetch_all_publishable_merged_items,
            side_effect=fetch_all_publishable_merged_items,
        ),
    )
    monkeypatch.setattr(
        BackendApiConnector, "_check_availability", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        BackendApiConnector,
        "system_status",
        MagicMock(
            return_value=VersionStatus.model_validate(
                {"status": "Fabulous", "version": "mex-backend-version"}
            )
        ),
    )
    monkeypatch.setattr(
        BackendApiConnector,
        "fetch_all_publishable_merged_items",
        backend.fetch_all_publishable_merged_items,
    )
    monkeypatch.setattr(
        BackendApiConnector,
        "fetch_publishable_merged_items",
        backend.fetch_publishable_merged_items,
    )
    return backend

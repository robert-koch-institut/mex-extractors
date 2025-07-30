from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    AnyMergedModel,
    ExtractedPrimarySource,
    MergedBibliographicResource,
    MergedConsent,
    MergedContactPoint,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedPrimarySource,
    PaginatedItemsContainer,
)
from mex.common.types import AccessRestriction


def fetch_merged_items(  # noqa: PLR0913
    *,
    query_string: str | None = None,  # noqa: ARG001
    identifier: str | None = None,  # noqa: ARG001
    entity_type: list[str] | None = None,
    referenced_identifier: list[str] | None = None,  # noqa: ARG001
    reference_field: str | None = None,  # noqa: ARG001
    skip: int = 0,  # noqa: ARG001
    limit: int = 100,  # noqa: ARG001
) -> list[AnyMergedModel]:
    merged_items: list[AnyMergedModel] = [
        MergedContactPoint(
            email=["mex@rki.de"],
            identifier="fakeFakeContact",
        ),
    ]
    items = [
        item
        for item in merged_items
        if not entity_type or item.entityType in entity_type
    ]
    return PaginatedItemsContainer[AnyMergedModel](total=len(items), items=items)


def fetch_all_merged_items(
    *,
    query_string: str | None = None,  # noqa: ARG001
    identifier: str | None = None,  # noqa: ARG001
    entity_type: list[str] | None = None,
    referenced_identifier: list[str] | None = None,  # noqa: ARG001
    reference_field: str | None = None,  # noqa: ARG001
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
            identifier="fakeFakeConsent",
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
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


def fetch_extracted_items(
    *,
    query_string: str | None = None,  # noqa: ARG001
    stable_target_id: str | None = None,  # noqa: ARG001
    entity_type: list[str] | None = None,
    skip: int = 0,  # noqa: ARG001
    limit: int = 100,  # noqa: ARG001
) -> PaginatedItemsContainer[AnyExtractedModel]:
    extracted_items: list[AnyExtractedModel] = [
        ExtractedPrimarySource(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="wikidata",
        ),
        ExtractedPrimarySource(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="endnote",
        ),
    ]
    items = [
        item
        for item in extracted_items
        if not entity_type or item.entityType in entity_type
    ]
    return PaginatedItemsContainer[AnyExtractedModel](total=len(items), items=items)


@pytest.fixture
def mocked_backend(monkeypatch: MonkeyPatch) -> MagicMock:
    backend = MagicMock(
        fetch_merged_items=MagicMock(
            spec=BackendApiConnector.fetch_merged_items, side_effect=fetch_merged_items
        ),
        fetch_all_merged_items=MagicMock(
            spec=BackendApiConnector.fetch_all_merged_items,
            side_effect=fetch_all_merged_items,
        ),
        fetch_extracted_items=MagicMock(
            spec=BackendApiConnector.fetch_extracted_items,
            side_effect=fetch_extracted_items,
        ),
    )
    monkeypatch.setattr(
        BackendApiConnector, "_check_availability", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        BackendApiConnector, "fetch_merged_items", backend.fetch_merged_items
    )
    monkeypatch.setattr(
        BackendApiConnector, "fetch_all_merged_items", backend.fetch_all_merged_items
    )
    monkeypatch.setattr(
        BackendApiConnector, "fetch_extracted_items", backend.fetch_extracted_items
    )
    return backend

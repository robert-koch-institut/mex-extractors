from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    AnyExtractedModel,
    AnyMergedModel,
    ExtractedPrimarySource,
    MergedConsent,
    MergedContactPoint,
    MergedPerson,
    MergedPrimarySource,
    PaginatedItemsContainer,
)


@pytest.fixture
def mocked_backend(monkeypatch: MonkeyPatch) -> MagicMock:
    backend = MagicMock(
        fetch_merged_items=MagicMock(
            return_value=PaginatedItemsContainer[AnyMergedModel](
                total=5,
                items=[
                    MergedPrimarySource(
                        identifier="fakefakefakeJA",
                    ),
                    MergedContactPoint(
                        email=["1fake@e.mail"],
                        identifier="alsofakefakefakeJA",
                    ),
                    MergedConsent(
                        identifier="anotherfakefakefakefak",
                        hasConsentStatus="https://mex.rki.de/item/consent-status-1",
                        hasDataSubject="fakefakefakefakefakefa",
                        isIndicatedAtTime="2014-05-21T19:38:51Z",
                    ),
                    MergedContactPoint(
                        email=["2fake@e.mail"],
                        identifier="alsofakefakefakeYO",
                    ),
                    MergedPerson(
                        fullName="Dr. Fake",
                        identifier="drdrdrdrdrdrfAKE",
                    ),
                ],
            )
        ),
        fetch_extracted_items=MagicMock(
            return_value=PaginatedItemsContainer[AnyExtractedModel](
                total=1,
                items=[
                    ExtractedPrimarySource(
                        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                        identifierInPrimarySource="endnote",
                    ),
                ],
            )
        ),
    )
    monkeypatch.setattr(
        BackendApiConnector, "_check_availability", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        BackendApiConnector, "fetch_merged_items", backend.fetch_merged_items
    )
    monkeypatch.setattr(
        BackendApiConnector, "fetch_extracted_items", backend.fetch_extracted_items
    )
    return backend

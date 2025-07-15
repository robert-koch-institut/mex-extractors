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
def mocked_backend(monkeypatch: MonkeyPatch) -> None:
    def mocked_fetch_merged_items(
        _self: BackendApiConnector, *_args: object
    ) -> PaginatedItemsContainer[AnyMergedModel]:
        return PaginatedItemsContainer[AnyMergedModel](
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

    monkeypatch.setattr(
        BackendApiConnector, "fetch_merged_items", mocked_fetch_merged_items
    )

    def mocked_fetch_extracted_items(
        _self: BackendApiConnector, *_args: object
    ) -> PaginatedItemsContainer[AnyExtractedModel]:
        return PaginatedItemsContainer[AnyExtractedModel](
            total=1,
            items=[
                ExtractedPrimarySource(
                    hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
                    identifierInPrimarySource="endnote",
                ),
            ],
        )

    monkeypatch.setattr(
        BackendApiConnector, "fetch_extracted_items", mocked_fetch_extracted_items
    )

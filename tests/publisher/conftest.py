from typing import Any

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import (
    AnyMergedModel,
    MergedConsent,
    MergedContactPoint,
    MergedPrimarySource,
    PaginatedItemsContainer,
)


@pytest.fixture
def mocked_backend(monkeypatch: MonkeyPatch) -> None:
    def mocked_request(
        _self: BackendApiConnector,
        _method: str = "GET",
        _endpoint: str | None = None,
        _payload: Any = None,  # noqa: ANN401
        _params: dict[str, str] | None = None,
        **_kwargs: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        return PaginatedItemsContainer[AnyMergedModel](
            total=1,
            items=[
                MergedPrimarySource(
                    entityType="MergedPrimarySource",
                    identifier="fakefakefakeJA",
                ),
                MergedContactPoint(
                    email=["1fake@e.mail"],
                    entityType="MergedContactPoint",
                    identifier="alsofakefakefakeJA",
                ),
                MergedConsent(
                    entityType="MergedConsent",
                    identifier="anotherfakefakefakefak",
                    hasConsentStatus="https://mex.rki.de/item/consent-status-1",
                    hasDataSubject="fakefakefakefakefakefa",
                    isIndicatedAtTime="2014-05-21T19:38:51Z",
                ),
                MergedContactPoint(
                    email=["2fake@e.mail"],
                    entityType="MergedContactPoint",
                    identifier="alsofakefakefakeYO",
                ),
            ],
        ).model_dump()

    monkeypatch.setattr(BackendApiConnector, "request", mocked_request)

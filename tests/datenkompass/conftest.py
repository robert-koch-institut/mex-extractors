from typing import Any

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import (
    AnyMergedModel,
    MergedActivity,
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
                MergedActivity(
                    entityType="MergedActivity",
                    identifier="fakefakefakeJA",
                    contact="12345678901234",
                    responsibleUnit="012345678901234",
                    title="This is a test",
                ),
            ],
        ).model_dump()

    monkeypatch.setattr(BackendApiConnector, "request", mocked_request)

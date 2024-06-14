from typing import Any

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector


@pytest.fixture
def mocked_backend(monkeypatch: MonkeyPatch) -> None:
    def mocked_request(
        self: BackendApiConnector,
        method: str,
        endpoint: str | None = None,
        payload: Any = None,
        params: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {"total": 0, "items": []}

    monkeypatch.setattr(BackendApiConnector, "request", mocked_request)

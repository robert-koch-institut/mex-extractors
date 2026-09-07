from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api import BackendApiConnector
from mex.common.models import VersionStatus


@pytest.fixture
def mocked_backend_s3(monkeypatch: MonkeyPatch) -> MagicMock:
    backend = MagicMock()
    backend.system_status.return_value = VersionStatus.model_validate(
        {"status": "Fabulous", "version": "mex-backend-version"}
    )

    monkeypatch.setattr(
        BackendApiConnector,
        "get",
        MagicMock(return_value=backend),
    )

    return backend

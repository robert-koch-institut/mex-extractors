from typing import TYPE_CHECKING

import pytest

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.types import IdentityProvider

if TYPE_CHECKING:
    from mex.extractors.settings import ExtractorsSettings


@pytest.fixture(autouse=True)
def use_backend_identity_provider(settings: ExtractorsSettings) -> None:
    """Assign identities via the backend, which rejects ids it did not hand out."""
    settings.identity_provider = IdentityProvider.BACKEND


@pytest.fixture(autouse=True)
def isolate_backend_graph(is_integration_test: bool) -> None:  # noqa: FBT001
    """Flush the graph database before every test, same as mex-backend's own suite."""
    if is_integration_test:
        BackendApiConnector.get().flush_graph()

import pytest

from mex.common.backend_api.connector import BackendApiConnector


@pytest.fixture(autouse=True)
def isolate_backend_graph(is_integration_test: bool) -> None:  # noqa: FBT001
    """Flush the graph database before every test, same as mex-backend's own suite."""
    if is_integration_test:
        BackendApiConnector.get().flush_graph()

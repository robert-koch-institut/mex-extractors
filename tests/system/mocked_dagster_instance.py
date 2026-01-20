from collections.abc import Iterator

import dagster
import pytest
from pytest import MonkeyPatch


@pytest.fixture(autouse=True)
def mocked_dagster_instance(
    monkeypatch: MonkeyPatch,
) -> Iterator[dagster.DagsterInstance]:
    """Mock DagsterInstance.get() for tests/test_main.py."""
    with dagster.DagsterInstance.ephemeral() as instance:
        monkeypatch.setattr("dagster.DagsterInstance.get", lambda: instance)
        yield instance

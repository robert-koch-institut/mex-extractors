from typing import TYPE_CHECKING

import dagster
import pytest
from pytest import MonkeyPatch

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def mocked_dagster_instance(
    monkeypatch: MonkeyPatch,
) -> Iterator[dagster.DagsterInstance]:
    """Mock DagsterInstance.get() for tests/test_main.py."""
    with dagster.DagsterInstance.ephemeral() as instance:
        monkeypatch.setattr("dagster.DagsterInstance.get", lambda: instance)
        yield instance

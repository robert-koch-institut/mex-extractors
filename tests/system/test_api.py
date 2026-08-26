from importlib.metadata import version
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch
from sqlalchemy.exc import SQLAlchemyError
from starlette.requests import Request

from mex.common.models import VersionStatus
from mex.extractors.system.api import (
    build_system_routes,
    get_daemon_status,
    get_postgres_status,
    get_system_status,
    patch_dagster_webserver,
    run_webserver,
)

if TYPE_CHECKING:
    from dagster import DagsterInstance


@pytest.fixture
def mocked_postgres_storage(monkeypatch: MonkeyPatch) -> MagicMock:
    """Make the instance look postgres-backed, reporting a fixed server version."""
    storage = MagicMock()
    connection = storage._engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar.return_value = "16.15 (Debian 16.15-1)"
    monkeypatch.setattr(
        "mex.extractors.system.api.PostgresRunStorage", MagicMock, raising=False
    )
    return storage


@pytest.fixture
def mocked_postgres_ok(monkeypatch: MonkeyPatch) -> None:
    """Report postgres as reachable, so the daemon checks are not short-circuited."""
    monkeypatch.setattr(
        "mex.extractors.system.api.get_postgres_status",
        lambda _instance: VersionStatus(status="ok", version="16.15"),
    )


def _mock_daemon_statuses(**healthy_by_type: bool | None) -> dict[str, MagicMock]:
    """Build a mocked mapping of daemon type to daemon status."""
    statuses = {}
    for daemon_type, healthy in healthy_by_type.items():
        status = MagicMock()
        status.healthy = healthy
        statuses[daemon_type] = status
    return statuses


def test_get_system_status() -> None:
    status = get_system_status()

    assert status.status == "ok"
    assert status.version


def test_get_postgres_status_not_postgres(
    mocked_dagster_instance: DagsterInstance,
) -> None:
    # the ephemeral test instance is not backed by postgres at all
    status = get_postgres_status(mocked_dagster_instance)

    assert status.status == "error"
    assert status.version == "unknown"


def test_get_postgres_status_ok(
    mocked_dagster_instance: DagsterInstance,
    mocked_postgres_storage: MagicMock,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        type(mocked_dagster_instance),
        "run_storage",
        property(lambda _self: mocked_postgres_storage),
    )

    status = get_postgres_status(mocked_dagster_instance)

    # the build details are dropped, only the version number is reported
    assert status.status == "ok"
    assert status.version == "16.15"


def test_get_postgres_status_offline(
    mocked_dagster_instance: DagsterInstance,
    mocked_postgres_storage: MagicMock,
    monkeypatch: MonkeyPatch,
) -> None:
    mocked_postgres_storage._engine.connect.side_effect = SQLAlchemyError("no route")
    monkeypatch.setattr(
        type(mocked_dagster_instance),
        "run_storage",
        property(lambda _self: mocked_postgres_storage),
    )

    status = get_postgres_status(mocked_dagster_instance)

    # a configured but unreachable server is offline, not misconfigured
    assert status.status == "offline"
    assert status.version == "unknown"


@pytest.mark.usefixtures("mocked_postgres_ok")
def test_get_daemon_status_ok(
    mocked_dagster_instance: DagsterInstance, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mocked_dagster_instance,
        "get_daemon_statuses",
        lambda: _mock_daemon_statuses(SENSOR=True, SCHEDULER=True),
    )

    status = get_daemon_status(mocked_dagster_instance)

    assert status.status == "ok"
    assert status.version == version("mex-extractors")


@pytest.mark.usefixtures("mocked_postgres_ok")
def test_get_daemon_status_one_daemon_down(
    mocked_dagster_instance: DagsterInstance, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mocked_dagster_instance,
        "get_daemon_statuses",
        lambda: _mock_daemon_statuses(SENSOR=True, SCHEDULER=False),
    )

    status = get_daemon_status(mocked_dagster_instance)

    # a single dead daemon is enough to stop schedules or sensors
    assert status.status == "offline"
    assert status.version == "unknown"


@pytest.mark.usefixtures("mocked_postgres_ok")
def test_get_daemon_status_never_reported(
    mocked_dagster_instance: DagsterInstance, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mocked_dagster_instance,
        "get_daemon_statuses",
        lambda: _mock_daemon_statuses(SENSOR=None),
    )

    status = get_daemon_status(mocked_dagster_instance)

    # a daemon that never sent a heartbeat is not healthy
    assert status.status == "offline"
    assert status.version == "unknown"


@pytest.mark.usefixtures("mocked_postgres_ok")
def test_get_daemon_status_none_required(
    mocked_dagster_instance: DagsterInstance,
) -> None:
    # the ephemeral test instance requires no daemons at all
    status = get_daemon_status(mocked_dagster_instance)

    assert status.status == "error"
    assert status.version == "unknown"


@pytest.mark.usefixtures("mocked_postgres_ok")
def test_get_daemon_status_heartbeats_unreadable(
    mocked_dagster_instance: DagsterInstance, monkeypatch: MonkeyPatch
) -> None:
    def raise_error() -> None:
        msg = "run storage went away mid-check"
        raise SQLAlchemyError(msg)

    monkeypatch.setattr(mocked_dagster_instance, "get_daemon_statuses", raise_error)

    status = get_daemon_status(mocked_dagster_instance)

    assert status.status == "error"
    assert status.version == "unknown"


@pytest.mark.parametrize("postgres_status", ["error", "offline"])
def test_get_daemon_status_without_postgres(
    mocked_dagster_instance: DagsterInstance,
    monkeypatch: MonkeyPatch,
    postgres_status: str,
) -> None:
    monkeypatch.setattr(
        "mex.extractors.system.api.get_postgres_status",
        lambda _instance: VersionStatus(status=postgres_status, version="unknown"),
    )
    daemon_health = MagicMock()
    monkeypatch.setattr("mex.extractors.system.api.get_daemon_health", daemon_health)

    status = get_daemon_status(mocked_dagster_instance)

    # heartbeats live in the run storage, so daemon health is unknowable without it
    assert status.status == "error"
    assert status.version == "unknown"
    # and we must not even try to read them: that retries the connection for over 10s
    daemon_health.assert_not_called()


def test_build_system_routes(mocked_dagster_instance: DagsterInstance) -> None:
    routes = build_system_routes(mocked_dagster_instance)

    assert [route.path for route in routes] == [
        "/_system/check",
        "/_system/postgres",
        "/_system/daemon",
    ]


def test_system_routes_respond(
    mocked_dagster_instance: DagsterInstance,
) -> None:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})

    for route in build_system_routes(mocked_dagster_instance):
        response = route.endpoint(request)

        assert response.status_code == 200, route.path
        assert response.body, route.path


def test_patch_dagster_webserver(
    mocked_dagster_instance: DagsterInstance, monkeypatch: MonkeyPatch
) -> None:
    class MockProcessContext:
        instance = mocked_dagster_instance

    class MockWebserver:
        _process_context = MockProcessContext()

        def build_routes(self) -> list[object]:
            return ["original-route"]

    monkeypatch.setattr(
        "mex.extractors.system.api.DagsterWebserver", MockWebserver, raising=False
    )
    patch_dagster_webserver()

    routes = MockWebserver().build_routes()

    # our routes are prepended, ahead of the webserver's catch-all index route
    assert [getattr(route, "path", route) for route in routes] == [
        "/_system/check",
        "/_system/postgres",
        "/_system/daemon",
        "original-route",
    ]


def test_run_webserver(monkeypatch: MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "mex.extractors.system.api.patch_dagster_webserver",
        lambda: calls.append("patched"),
    )
    monkeypatch.setattr(
        "mex.extractors.system.api.dagster_webserver_main",
        lambda: calls.append("served"),
    )

    run_webserver()

    assert calls == ["patched", "served"]

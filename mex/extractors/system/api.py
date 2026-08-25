from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from dagster_postgres.run_storage import PostgresRunStorage
from dagster_postgres.utils import DagsterPostgresException
from dagster_webserver.cli import main as dagster_webserver_main
from dagster_webserver.webserver import DagsterWebserver
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from mex.common.logging import logger
from mex.common.models import VersionStatus

if TYPE_CHECKING:
    from dagster import DagsterInstance
    from starlette.requests import Request
    from starlette.routing import BaseRoute


def get_system_status() -> VersionStatus:
    """Get the status and version of the extractors webserver."""
    return VersionStatus(status="ok", version=version("mex-extractors"))


def get_postgres_status(instance: DagsterInstance) -> VersionStatus:
    """Get the status and version of the postgres database."""
    run_storage = instance.run_storage
    if not isinstance(run_storage, PostgresRunStorage):
        return VersionStatus(status="error", version="unknown")
    try:
        # deliberately bypassing `run_storage.connect()`, which wraps every connection
        # in `retry_pg_connection_fn` and the health check would take over 10s.
        with run_storage._engine.connect() as connection:  # noqa: SLF001
            server_version = connection.execute(text("SHOW server_version")).scalar()
    except DagsterPostgresException, SQLAlchemyError:
        logger.exception("error checking the postgres database status")
        return VersionStatus(status="offline", version="unknown")
    # strip the postgres build details from the version, e.g. "16.15 (Debian ...)"
    return VersionStatus(status="ok", version=str(server_version).split(" ")[0])


def get_daemon_health(instance: DagsterInstance) -> dict[str, bool] | None:
    """Check whether each required dagster daemon has sent a recent heartbeat."""
    try:
        return {
            daemon_type: bool(status.healthy)
            for daemon_type, status in instance.get_daemon_statuses().items()
        }
    except DagsterPostgresException, SQLAlchemyError:
        logger.exception("error checking the dagster daemon status")
        return None


def get_daemon_status(instance: DagsterInstance) -> VersionStatus:
    """Get the status and version of the dagster daemons."""
    if get_postgres_status(instance).status != "ok":
        # exit early if we can't even access postgres, because reading heartbeats
        # retries the connection and would take over 10s to report an outage
        return VersionStatus(status="error", version="unknown")
    if not (daemons := get_daemon_health(instance)):
        return VersionStatus(status="error", version="unknown")
    if unhealthy := sorted(name for name, up in daemons.items() if not up):
        logger.error("unhealthy daemons: %s", ", ".join(unhealthy))
        return VersionStatus(status="offline", version="unknown")
    return VersionStatus(status="ok", version=version("mex-extractors"))


def build_system_routes(instance: DagsterInstance) -> list[Route]:
    """Build the mex system routes to serve on the dagster webserver."""

    def check_system_status(_request: Request) -> Response:
        return JSONResponse(get_system_status().model_dump())

    def check_daemon_status(_request: Request) -> Response:
        return JSONResponse(get_daemon_status(instance).model_dump())

    def check_postgres_status(_request: Request) -> Response:
        return JSONResponse(get_postgres_status(instance).model_dump())

    return [
        Route("/_system/check", check_system_status),
        Route("/_system/postgres", check_postgres_status),
        Route("/_system/daemon", check_daemon_status),
    ]


def patch_dagster_webserver() -> None:
    """Patch the dagster webserver to also serve the mex system routes."""
    if getattr(DagsterWebserver.build_routes, "_mex_system_routes_patched", False):
        return

    original_build_routes = DagsterWebserver.build_routes

    def build_routes(self: DagsterWebserver[Any, Any]) -> list[BaseRoute]:
        instance = self._process_context.instance  # noqa: SLF001
        original_routes = original_build_routes(self)  # type: ignore[no-untyped-call]
        return [*build_system_routes(instance), *original_routes]

    build_routes._mex_system_routes_patched = True  # type: ignore[attr-defined]
    DagsterWebserver.build_routes = build_routes  # type: ignore[method-assign]


def run_webserver() -> None:
    """Run the dagster webserver with the mex system routes."""
    patch_dagster_webserver()
    dagster_webserver_main()  # type: ignore[no-untyped-call]

from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from dagster import DagsterError, DagsterInstance
from dagster import __version__ as dagster_version
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
    from starlette.requests import Request
    from starlette.routing import BaseRoute

# errors raised when dagster cannot reach its configured storage backend
STORAGE_ERRORS = (DagsterError, DagsterPostgresException, SQLAlchemyError)


def get_system_status() -> VersionStatus:
    """Get the status and version of the extractors webserver.

    Returns:
        VersionStatus with status "ok" and the mex-extractors version
    """
    return VersionStatus(status="ok", version=version("mex-extractors"))


def get_postgres_status(instance: DagsterInstance) -> VersionStatus:
    """Get the status and version of the postgres database.

    Returns:
        VersionStatus with status "ok" and the postgres server version, status
        "local" when dagster is not configured with postgres storage, or status
        "error" when the configured postgres server is unreachable
    """
    run_storage = instance.run_storage
    if not isinstance(run_storage, PostgresRunStorage):
        return VersionStatus(status="local", version="unknown")
    try:
        # deliberately bypassing `run_storage.connect()`, which wraps every connection
        # in `retry_pg_connection_fn` (5 attempts, exponential backoff). That makes an
        # outage take ~11s to report, past the timeout of most health probes, so a
        # monitor would see a timeout instead of our "error" status.
        with run_storage._engine.connect() as connection:  # noqa: SLF001
            server_version = connection.execute(text("SHOW server_version")).scalar()
    except STORAGE_ERRORS:
        logger.exception("error checking the postgres database status")
        return VersionStatus(status="error", version="unknown")
    # postgres appends build details to the server version, e.g. "16.15 (Debian ...)",
    # which we drop to avoid leaking build info on an unauthenticated endpoint
    parsed_version = str(server_version).split(" ")[0] if server_version else None
    return VersionStatus(status="ok", version=parsed_version or "unknown")


def get_daemon_status(instance: DagsterInstance) -> VersionStatus:
    """Get the status and version of the dagster daemons.

    Every required daemon has to be healthy for this to report "ok" - a single dead
    daemon silently stops schedules or sensors, so a partial outage is still an error.
    Which daemon is at fault is written to the log. Daemon heartbeats live in the
    shared run storage, so their health is unknowable while that storage is
    unreachable.

    Returns:
        VersionStatus with status "ok" and the dagster version when every required
        daemon is healthy, status "local" when no daemons are required at all, or
        status "error" when a required daemon is missing its heartbeat
    """
    if get_postgres_status(instance).status == "error":
        return VersionStatus(status="error", version="unknown")
    daemons = _get_daemon_health(instance)
    if daemons is None:
        return VersionStatus(status="error", version="unknown")
    if not daemons:
        # an instance without required daemons has nothing to report on, e.g. in tests
        return VersionStatus(status="local", version="unknown")
    if unhealthy := sorted(name for name, up in daemons.items() if not up):
        logger.error("unhealthy dagster daemons: %s", ", ".join(unhealthy))
        return VersionStatus(status="error", version="unknown")
    return VersionStatus(status="ok", version=dagster_version)


def _get_daemon_health(instance: DagsterInstance) -> dict[str, bool] | None:
    """Check whether each required dagster daemon has sent a recent heartbeat.

    Returns:
        Mapping of daemon type to health, empty when no daemons are required at all,
        or None when the heartbeats could not be read
    """
    try:
        return {
            daemon_type: bool(status.healthy)
            for daemon_type, status in instance.get_daemon_statuses().items()
        }
    except STORAGE_ERRORS:
        logger.exception("error checking the dagster daemon status")
        return None


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
    """Patch the dagster webserver to also serve the mex system routes.

    The webserver has no supported extension point for adding routes, so we wrap
    `DagsterWebserver.build_routes` and prepend our own. The system routes are
    prepended so they take precedence over the catch-all index route, and they are
    added outside of any configured `--path-prefix` mount, so that health probes can
    always reach them at a stable path.
    """
    original_build_routes = DagsterWebserver.build_routes

    def build_routes(self: DagsterWebserver[Any, Any]) -> list[BaseRoute]:
        instance = self._process_context.instance
        original_routes = original_build_routes(self)  # type: ignore[no-untyped-call]
        return [*build_system_routes(instance), *original_routes]

    DagsterWebserver.build_routes = build_routes  # type: ignore[method-assign]


def run() -> None:
    """Run the dagster webserver with the mex system routes."""
    patch_dagster_webserver()
    dagster_webserver_main()  # type: ignore[no-untyped-call]

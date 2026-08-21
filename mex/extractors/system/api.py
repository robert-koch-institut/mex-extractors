from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from dagster import DagsterError, DagsterInstance, DagsterRunStatus, RunsFilter
from dagster import __version__ as dagster_version
from dagster_postgres.run_storage import PostgresRunStorage
from dagster_postgres.utils import DagsterPostgresException
from dagster_webserver.cli import main as dagster_webserver_main
from dagster_webserver.webserver import DagsterWebserver
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from mex.common.connector import CONNECTOR_STORE
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
        # outage take ~11s to report, past the default prometheus scrape timeout, so
        # a monitor would see a timeout instead of our "error" status.
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
    Which daemon is at fault is reported per daemon by `dagster_daemon_up` on the
    metrics endpoint. Daemon heartbeats live in the shared run storage, so their
    health is unknowable while that storage is unreachable.

    Returns:
        VersionStatus with status "ok" and the dagster version when every required
        daemon is healthy, status "local" when no daemons are required at all, or
        status "error" when a required daemon is missing its heartbeat
    """
    if get_postgres_status(instance).status == "error":
        return VersionStatus(status="error", version="unknown")
    daemons = _get_daemon_metrics(instance)
    if not daemons:
        # an instance without required daemons has nothing to report on, e.g. in tests
        return VersionStatus(status="local", version="unknown")
    if unhealthy := sorted(name for name, up in daemons.items() if not up):
        logger.error("unhealthy dagster daemons: %s", ", ".join(unhealthy))
        return VersionStatus(status="error", version="unknown")
    return VersionStatus(status="ok", version=dagster_version)


def _get_daemon_metrics(instance: DagsterInstance) -> dict[str, int]:
    """Check whether each required dagster daemon has sent a recent heartbeat."""
    try:
        return {
            daemon_type: int(bool(status.healthy))
            for daemon_type, status in instance.get_daemon_statuses().items()
        }
    except STORAGE_ERRORS:
        logger.exception("error checking the dagster daemon status")
        return {}


def _render_metric_family(
    name: str, metric_type: str, samples: dict[str, int], label: str
) -> str:
    """Render one labelled metric family, with a single shared TYPE line."""
    if not samples:
        return ""
    lines = [f"# TYPE {name} {metric_type}"]
    lines += [f'{name}{{{label}="{key}"}} {value}' for key, value in samples.items()]
    return "\n".join(lines)


def _render_metrics(metrics: dict[str, int], metric_type: str) -> str:
    """Render a mapping of metrics in the prometheus text exposition format."""
    return "\n\n".join(
        f"# TYPE {key} {metric_type}\n{key} {value}" for key, value in metrics.items()
    )


def _get_dagster_run_metrics(instance: DagsterInstance) -> dict[str, int]:
    """Count the dagster runs currently known per run status."""
    try:
        return {
            f"dagster_runs_{status.value.lower()}": instance.get_runs_count(
                RunsFilter(statuses=[status])
            )
            for status in DagsterRunStatus
        }
    except STORAGE_ERRORS:
        logger.exception("error collecting dagster run metrics")
        return {}


def get_prometheus_metrics(instance: DagsterInstance) -> str:
    """Get connector and dagster run metrics in the prometheus text format."""
    # querying run counts while the storage is down takes ~11s, because dagster retries
    # every connection with exponential backoff. That is longer than the default
    # prometheus scrape timeout, so probe the storage first and skip the run counts
    # when it is unreachable, letting the scrape carry `dagster_storage_up 0` instead.
    storage_is_up = get_postgres_status(instance).status != "error"
    run_gauges = _get_dagster_run_metrics(instance) if storage_is_up else {}
    daemon_gauges = _get_daemon_metrics(instance) if storage_is_up else {}
    sections = (
        _render_metrics(CONNECTOR_STORE.metrics(), "counter"),
        _render_metrics({"dagster_storage_up": int(storage_is_up)}, "gauge"),
        _render_metric_family("dagster_daemon_up", "gauge", daemon_gauges, "daemon"),
        _render_metrics(run_gauges, "gauge"),
    )
    return "\n\n".join(section for section in sections if section)


def build_system_routes(instance: DagsterInstance) -> list[Route]:
    """Build the mex system routes to serve on the dagster webserver."""

    def check_system_status(_request: Request) -> Response:
        return JSONResponse(get_system_status().model_dump())

    def check_daemon_status(_request: Request) -> Response:
        return JSONResponse(get_daemon_status(instance).model_dump())

    def check_postgres_status(_request: Request) -> Response:
        return JSONResponse(get_postgres_status(instance).model_dump())

    def check_prometheus_metrics(_request: Request) -> Response:
        return PlainTextResponse(get_prometheus_metrics(instance))

    return [
        Route("/_system/check", check_system_status),
        Route("/_system/postgres", check_postgres_status),
        Route("/_system/daemon", check_daemon_status),
        Route("/_system/metrics", check_prometheus_metrics),
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

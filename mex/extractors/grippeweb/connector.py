import contextlib
import platform
from subprocess import PIPE, STDOUT, Popen
from typing import Any, cast

import backoff

# https://github.com/mkleehammer/pyodbc/wiki/Install#installing-on-linux
import pyodbc  # type: ignore[import-not-found]

from mex.common.connector import BaseConnector
from mex.common.logging import logger
from mex.extractors.settings import ExtractorsSettings

QUERY_BY_TABLE_NAME = {
    "vActualQuestion": "SELECT * FROM GrippeWeb.MEx.vActualQuestion",
    "vMasterDataMEx": "SELECT * FROM GrippeWeb.MEx.vMasterDataMEx",
    "vWeeklyResponsesMEx": "SELECT * FROM GrippeWeb.MEx.vWeeklyResponsesMEx",
}


class GrippewebConnector(BaseConnector):
    """Connector to handle authentication and queries for Grippeweb SQL server."""

    def __init__(self) -> None:
        """Create a new connector instance."""
        self._connection = self._setup_connection()

    def _setup_connection(self) -> pyodbc.Connection:
        """Set up a new pyodbc connection, refreshing the Kerberos ticket if needed."""
        settings = ExtractorsSettings.get()
        if platform.system() != "Windows":  # pragma: no cover
            process = Popen(  # noqa: S603
                ["kinit", settings.kerberos_user, "-V"],  # noqa: S607
                stdout=PIPE,
                stdin=PIPE,
                stderr=STDOUT,
                encoding="utf-8",
            )
            stdout, stderr = process.communicate(
                input=settings.kerberos_password.get_secret_value()
            )
            logger.info(stdout)
            if stderr:
                logger.error(stderr)

        return pyodbc.connect(settings.grippeweb.mssql_connection_dsn)

    def reconnect(self) -> None:
        """Close current connection and initiate a new one."""
        self.close()
        self._connection = self._setup_connection()

    @backoff.on_exception(
        wait_gen=backoff.fibo,
        exception=pyodbc.Error,  # Catches OperationalError, DatabaseError, etc.
        max_tries=2,
        logger=logger,
        on_backoff=lambda details: cast(
            "GrippewebConnector", details["args"][0]
        ).reconnect(),
    )
    def parse_columns_by_column_name(self, table_name: str) -> dict[str, list[Any]]:
        """Execute whitelisted queries and zip results to column name."""
        with self._connection.cursor() as cursor:
            cursor.execute(QUERY_BY_TABLE_NAME[table_name])
            table = cursor.fetchall()
            table_columns = list(zip(*table, strict=False))
            return {
                column_name[0]: list(column)
                for column_name, column in zip(
                    cursor.description, table_columns, strict=False
                )
            }

    def close(self) -> None:
        """Close the underlying connection."""
        if getattr(self, "_connection", None):
            with contextlib.suppress(pyodbc.Error):
                self._connection.close()

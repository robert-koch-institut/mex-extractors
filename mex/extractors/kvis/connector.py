import platform
from subprocess import PIPE, STDOUT, Popen
from typing import Any

from pydantic import BaseModel

from mex.common.connector import BaseConnector
from mex.common.logging import logger
from mex.extractors.kvis.models.table_models import (
    KVISFieldValuesTable,
    KVISVariablesTable,
)
from mex.extractors.settings import Settings

QUERY_BY_MODEL = {
    KVISVariablesTable: "SELECT * FROM KVIS.Mex.vKVISVariables",
    KVISFieldValuesTable: "SELECT * FROM KVIS.Mex.vKVISFieldValues",
}


class KVISConnector(BaseConnector):
    """Connector to handle authentication and queries for KVIS SQL server."""

    def __init__(self) -> None:
        """Create a new connector instance."""
        # https://github.com/mkleehammer/pyodbc/wiki/Install#installing-on-linux
        import pyodbc  # type: ignore[import-not-found]  # noqa: PLC0415

        settings = Settings.get()
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
            logger.error(stderr)
        self._connection = pyodbc.connect(settings.kvis.mssql_connection_dsn)

    def parse_rows(self, model: type[BaseModel]) -> list[dict[str, Any]]:
        """Execute whitelisted queries and zip results to column name."""
        with self._connection.cursor() as cursor:
            cursor.execute(QUERY_BY_MODEL[model])
            result = cursor.fetchall()
            return [
                dict(
                    zip([column[0] for column in cursor.description], row, strict=False)
                )
                for row in result
            ]

    def close(self) -> None:
        """Close the underlying connection."""
        self._connection.close()

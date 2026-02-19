import platform
from subprocess import PIPE, STDOUT, Popen
from typing import TYPE_CHECKING, Any

from mex.common.connector import BaseConnector
from mex.common.logging import logger
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_datatype import MetaDataType
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from pydantic import BaseModel


class NoOpPyodbc:
    """No-op pyodbc drop-in for when the libodbc dependency is not installed."""

    def connect(self, _: str) -> None:  # pragma: no cover
        """Create a new ODBC connection to a database."""
        return


try:
    import pyodbc  # type: ignore[import-not-found]
except ImportError:
    pyodbc = NoOpPyodbc


QUERY_BY_MODEL = {
    MetaCatalogue2Item: "SELECT * FROM SurvNet3Meta.Meta.Catalogue2Item",
    MetaCatalogue2Item2Schema: "SELECT * FROM SurvNet3Meta.Meta.Catalogue2Item2Schema",
    MetaDataType: "SELECT * FROM SurvNet3Meta.Meta.DataType",
    MetaDisease: "SELECT * FROM SurvNet3Meta.Meta.Disease",
    MetaField: "SELECT * FROM SurvNet3Meta.Meta.Field",
    MetaItem: "SELECT * FROM SurvNet3Meta.Meta.Item",
    MetaSchema2Field: "SELECT * FROM SurvNet3Meta.Meta.Schema2Field",
    MetaSchema2Type: "SELECT * FROM SurvNet3Meta.Meta.Schema2Type",
    MetaType: "SELECT * FROM SurvNet3Meta.Meta.Type",
}


class IFSGConnector(BaseConnector):
    """Connector to handle authentication and queries towards the IFSG SQL server."""

    def __init__(self) -> None:
        """Create a new connector instance."""
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
        self._connection = pyodbc.connect(settings.ifsg.mssql_connection_dsn)

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

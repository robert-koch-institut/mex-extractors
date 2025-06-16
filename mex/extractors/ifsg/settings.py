from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class IFSGSettings(BaseModel):
    """Settings submodel definition for the infection protection act data."""

    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/ifsg"),
        description=(
            "Path to the directory with the ifsg mapping files containing the default "
            "values, absolute path or relative to `assets_dir`."
        ),
    )
    mssql_connection_dsn: str = Field(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=domain.tld;DATABASE=database",
        description=(
            "Connection string for the ODBC Driver for SQL Server: "
            "https://learn.microsoft.com/en-us/sql/connect/odbc/"
            "dsn-connection-string-attribute"
        ),
    )

from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class KVISSettings(BaseModel):
    """Settings definition for the infection protection act data."""

    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/kvis"),
        description=(
            "Path to the directory with the KVIS mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
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
    kerberos_enabled: bool = Field(
        False,  # noqa: FBT003
        description=(
            "Whether to obtain a Kerberos ticket via kinit before connecting. "
            "Disabled by default (e.g. for SQL auth against a test server); enable "
            "it to authenticate against the real RKI SQL server."
        ),
    )

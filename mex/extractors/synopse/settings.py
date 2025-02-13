from pydantic import Field, SecretStr

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class SynopseSettings(BaseModel):
    """Synopse settings submodel definition for the Synopse extractor."""

    report_server_url: str = Field(
        "https://report-server/", description="Report Server instance URL"
    )
    report_server_username: SecretStr = Field(
        SecretStr("username"),
        description="Report Server user name",
    )
    report_server_password: SecretStr = Field(
        SecretStr("password"),
        description="Report Server password",
    )
    variablenuebersicht_path: AssetsPath = Field(
        AssetsPath("raw-data/synopse/variablenuebersicht.csv"),
        description=(
            "Path of the export in CSV format, absolute or relative to `asset_dir`"
        ),
    )
    projekt_und_studienverwaltung_path: AssetsPath = Field(
        AssetsPath("raw-data/synopse/projekt_und_studienverwaltung.csv"),
        description=(
            "Path of the export in CSV format, absolute or relative to `asset_dir`"
        ),
    )
    metadaten_zu_datensaetzen_path: AssetsPath = Field(
        AssetsPath("raw-data/synopse/metadaten_zu_datensaetzen.csv"),
        description=(
            "Path of the export in CSV format, absolute or relative to `asset_dir`"
        ),
    )
    datensatzuebersicht_path: AssetsPath = Field(
        AssetsPath("raw-data/synopse/datensatzuebersicht.csv"),
        description=(
            "Path of the export in CSV format, absolute or relative to `asset_dir`"
        ),
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/synopse"),
        description=(
            "Path to the directory with the synopse mapping files"
            "values, absolute path or relative to `assets_dir`."
        ),
    )

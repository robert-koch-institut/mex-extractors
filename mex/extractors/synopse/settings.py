from pydantic import Field, SecretStr

from mex.common.models import BaseModel


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
    variablenuebersicht_path: str = Field(
        "raw-data/synopse/variablenuebersicht.csv",
        description=("Path of the export in CSV format, relative to `asset_dir`"),
    )
    projekt_und_studienverwaltung_path: str = Field(
        "raw-data/synopse/projekt_und_studienverwaltung.csv",
        description=("Path of the export in CSV format, relative to `asset_dir`"),
    )
    metadaten_zu_datensaetzen_path: str = Field(
        "raw-data/synopse/metadaten_zu_datensaetzen.csv",
        description=("Path of the export in CSV format, relative to `asset_dir`"),
    )
    datensatzuebersicht_path: str = Field(
        "raw-data/synopse/datensatzuebersicht.csv",
        description=("Path of the export in CSV format, relative to `asset_dir`"),
    )
    mapping_path: str = Field(
        "mappings/synopse",
        description=(
            "Path to the directory with the synopse mapping files"
            "values, relative to `assets_dir`."
        ),
    )

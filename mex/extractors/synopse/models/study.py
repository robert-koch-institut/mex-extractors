from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class SynopseStudy(BaseModel):
    """Model for synopse study data with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    titel_datenset: str = Field(..., alias="Titel_Datenset")
    beschreibung: str | None = Field(None, alias="Beschreibung")
    schlagworte_themen: str | None = Field(None, alias="SchlagworteThemen")
    dateiformat: str | None = Field(None, alias="Dateiformat")
    dokumentation: str | None = Field(None, alias="Dokumentation")
    erstellungs_datum: str | None = Field(None, alias="ErstellungsDatum")
    version: str | None = Field(None, alias="Version")
    rechte: str | None = Field(None, alias="Rechte")
    lizenz: str | None = Field(None, alias="Lizenz")
    plattform: str | None = Field(None, alias="Plattform")
    plattform_adresse: str | None = Field(None, alias="PlattformAdresse")
    zugangsbeschraenkung: str | None = Field(None, alias="Zugangsbeschraenkung")
    studien_id: str = Field(..., alias="StudienID")
    ds_typ_id: int = Field(..., alias="DStypID")
    studie: str | None = Field(None, alias="Studie")

from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class SynopseStudy(BaseModel):
    """Model for synopse study data with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    beschreibung: str | None = Field(None, alias="Beschreibung")
    bevoelkerungsabdeckung: str | None = Field(None, alias="Bevoelkerungsabdeckung")
    dateiformat: str | None = Field(None, alias="Dateiformat")
    dokumentation: str | None = Field(None, alias="Dokumentation")
    ds_typ_id: int = Field(..., alias="DStypID")
    erstellungs_datum: str | None = Field(None, alias="ErstellungsDatum")
    feld_ende: str | None = Field(None, alias="Feldende")
    feld_start: str | None = Field(None, alias="Feldstart ")
    herkunft_der_daten: str | None = Field(None, alias="HerkunfDerDaten")
    lizenz: str | None = Field(None, alias="Lizenz")
    datum_der_letzten_aenderung: str | None = Field(
        None, alias="DatumDerLetztenAenderung"
    )
    plattform: str | None = Field(None, alias="Plattform")
    plattform_adresse: str | None = Field(None, alias="PlattformAdresse")
    rechte: str | None = Field(None, alias="Rechte")
    raeumlicher_bezug: str | None = Field(None, alias="RaeumlicherBezug")
    schlagworte_themen: str | None = Field(None, alias="SchlagworteThemen")
    studie: str | None = Field(None, alias="Studie")
    studien_id: str = Field(..., alias="StudienID")
    titel_datenset: str = Field(..., alias="Titel_Datenset")
    typisches_alter_max: str | None = Field(None, alias="TypischesAlterMax")
    typisches_alter_min: str | None = Field(None, alias="TypischesAlterMin")
    version: str | None = Field(None, alias="Version")
    zugangsbeschraenkung: str = Field(..., alias="Zugangsbeschraenkung")
    zweck: str | None = Field(None, alias="Zweck der Datenverarbeitung")

from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class MetadatenZuDatensaetzen(BaseModel):
    """Model for synopse study data with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    studien_id: str = Field(..., alias="StudienID")
    dstyp_id: int = Field(..., alias="DStypID")
    titel_datenset: str = Field(..., alias="Titel_Datenset")
    beschreibung: str | None = Field(None, alias="Beschreibung")
    schlagworte_themen: str | None = Field(None, alias="SchlagworteThemen")
    rechte: str | None = Field(None, alias="Rechte")
    zugangsbeschraenkung: str = Field(..., alias="Zugangsbeschraenkung")
    typisches_alter_max: int | None = Field(None, alias="TypischesAlterMax")
    typisches_alter_min: int | None = Field(None, alias="TypischesAlterMin")
    raeumlicher_bezug: str | None = Field(None, alias="RaeumlicherBezug")
    feldstart: str | None = Field(None, alias="Feldstart")
    feldende: str | None = Field(None, alias="Feldende")
    zweck_der_datenverarbeitung: str | None = Field(
        None, alias="ZweckDerDatenverarbeitung"
    )
    datum_der_letzten_aenderung: str | None = Field(
        None, alias="DatumDerLetztenAenderung"
    )
    bevoelkerungsabdeckung: str | None = Field(None, alias="Bevoelkerungsabdeckung")
    herkunft_der_daten: str | None = Field(None, alias="HerkunftDerDaten")

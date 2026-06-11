from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class MetadatenZuDatensaetzen(BaseModel):
    """Model for synopse study data with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    Beschreibung: str | None = None
    Bevoelkerungsabdeckung: str | None = None
    DStypID: int = Field(...)
    Feldende: str | None = None
    Feldstart: str | None = None
    HerkunftDerDaten: str | None = None
    DatumDerLetztenAenderung: str | None = None
    Rechte: str | None = None
    RaeumlicherBezug: str | None = None
    SchlagworteThemen: str | None = None
    Studie: str | None = None
    StudienID: str = Field(...)
    Titel_Datenset: str = Field(...)
    TypischesAlterMax: str | None = None
    TypischesAlterMin: str | None = None
    Zugangsbeschraenkung: str = Field(...)
    ZweckDerDatenverarbeitung: str | None = None

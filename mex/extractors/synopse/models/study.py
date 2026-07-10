from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class MetadatenZuDatensaetzen(BaseModel):
    """Model for synopse study data with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    StudienID: str = Field(...)
    DStypID: int = Field(...)
    Titel_Datenset: str = Field(...)
    Beschreibung: str | None = None
    SchlagworteThemen: str | None = None
    Rechte: str | None = None
    Zugangsbeschraenkung: str = Field(...)


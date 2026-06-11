from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class Datensatzuebersicht(BaseModel):
    """Model for synopse study overview with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    StudienID: str = Field(...)
    DStypID: int | None = Field(...)
    Titel_Datenset: str = Field(...)
    SynopseID: str = Field(...)

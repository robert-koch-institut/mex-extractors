from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class Datensatzuebersicht(BaseModel):
    """Model for synopse study overview with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    studien_id: str = Field(..., alias="StudienID")
    titel_datenset: str = Field(..., alias="Titel_Datenset")
    synopse_id: str = Field(..., alias="SynopseID")
    dstyp_id: int | None = Field(None, alias="DStypID")

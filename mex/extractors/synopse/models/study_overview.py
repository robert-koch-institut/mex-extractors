from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class SynopseStudyOverview(BaseModel):
    """Model for synopse study overview with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    studien_id: str = Field(..., alias="StudienID")
    ds_typ_id: int | None = Field(..., alias="DStypID")
    titel_datenset: str = Field(..., alias="Titel_Datenset")
    synopse_id: str = Field(..., alias="SynopseID")

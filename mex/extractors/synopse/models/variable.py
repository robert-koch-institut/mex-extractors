from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class SynopseVariable(BaseModel):
    """Model class for Synopse variables."""

    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    auspraegungen: str | None = Field(None, alias="textbox49")
    datentyp: str | None = Field(None, alias="textbox11")
    originalfrage: str | None = Field(None, alias="Originalfrage")
    studie_id: int = Field(..., alias="StudieID2")
    studie: str = Field(..., alias="StudieID1")
    synopse_id: str = Field(..., alias="SymopseID")
    text_dt: str | None = Field(None, alias="textbox51")
    thema_und_fragebogenausschnitt: str = Field(..., alias="textbox5")
    unterthema: str = Field(..., alias="textbox2")
    val_instrument: str | None = Field(None, alias="valInstrument")
    varlabel: str | None = Field(None, alias="textbox21")
    varname: str = Field(..., alias="textbox24")
    int_var: bool = Field(..., alias="IntVar")
    keep_varname: bool = Field(..., alias="KeepVarname")

from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class Variablenuebersicht(BaseModel):
    """Model class for Synopse variables."""

    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    textbox5: str = Field(..., alias="textbox5")
    val_instrument: str | None = Field(None, alias="valInstrument")
    textbox11: str | None = Field(None, alias="textbox11")
    originalfrage: str | None = Field(None, alias="Originalfrage")
    symopse_id: str = Field(..., alias="SymopseID")
    textbox21: str | None = Field(None, alias="textbox21")
    textbox24: str = Field(..., alias="textbox24")
    textbox51: str | None = Field(None, alias="textbox51")
    int_var: bool = Field(..., alias="IntVar")
    studie_id1: str = Field(..., alias="StudieID1")
    studie_id2: int = Field(..., alias="StudieID2")

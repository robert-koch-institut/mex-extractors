from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class Variablenuebersicht(BaseModel):
    """Model class for Synopse variables."""

    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    textbox5: str = Field(...)
    valInstrument: str | None = None
    textbox11: str | None = None
    Originalfrage: str | None = None
    SymopseID: str = Field(...)
    textbox21: str | None = None
    textbox24: str = Field(...)
    textbox51: str | None = None
    IntVar: bool = Field(...)
    StudieID1: str = Field(...)
    StudieID2: int = Field(...)


from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class Variablenuebersicht(BaseModel):
    """Model class for Synopse variables."""

    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)

    Auspraegungen: str | None = None
    Datentyp: str | None = None
    Originalfrage: str | None = None
    StudieID2: int = Field(...)
    StudieID1: str = Field(...)
    SymopseID: str = Field(...)
    textbox51: str | None = None
    textbox5: str = Field(...)
    textbox2: str = Field(...)
    valInstrument: str | None = None
    textbox21: str | None = None
    textbox24: str = Field(...)
    IntVar: bool = Field(...)
    KeepVarname: bool = Field(...)

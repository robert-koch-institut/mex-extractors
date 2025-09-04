from typing import Any

from mex.common.models import BaseModel


class IGSSchema(BaseModel):
    """Model class for IGS Schemas."""


class IGSEnumSchema(IGSSchema):
    """Model class for IGS Enum Schemas."""

    enum: list[str]


class IGSPropertiesSchema(IGSSchema):
    """Model class for IGS Properties Schemas."""

    properties: dict[str, Any]

class IGSInfo(BaseModel):
    """Model class for IGS Info."""
    title: str
    version: str

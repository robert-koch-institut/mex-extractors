from typing import Any

from mex.common.models import BaseModel


class IGSSchemas(BaseModel):
    """Model class for IGS Schemas."""


class IGSEnum(IGSSchemas):
    """Model class for IGS Enum."""

    enum: list[str]


class IGSProperties(IGSSchemas):
    """Model class for IGS Properties."""

    properties: dict[str, Any]

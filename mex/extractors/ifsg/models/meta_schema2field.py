from pydantic import Field

from mex.common.models import BaseModel


class MetaSchema2Field(BaseModel):
    """Model class for ifsg schema2field entities."""

    id_schema: int = Field(..., alias="IdSchema")
    id_field: int = Field(..., alias="IdField")

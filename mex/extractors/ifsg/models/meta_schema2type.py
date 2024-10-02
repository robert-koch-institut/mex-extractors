from pydantic import Field

from mex.common.models import BaseModel


class MetaSchema2Type(BaseModel):
    """Model class for ifsg schema2type entities."""

    id_schema: int = Field(..., alias="IdSchema")
    id_type: int = Field(..., alias="IdType")

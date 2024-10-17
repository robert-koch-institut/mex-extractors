from pydantic import Field

from mex.common.models import BaseModel


class MetaDataType(BaseModel):
    """Model class for ifsg MetaDataType entities."""

    id_data_type: int = Field(..., alias="IdDataType")
    data_type_name: str = Field(..., alias="DataTypeName")

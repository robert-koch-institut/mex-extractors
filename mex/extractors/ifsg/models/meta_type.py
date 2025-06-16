from pydantic import Field

from mex.common.models import BaseModel


class MetaType(BaseModel):
    """Model class for ifsg meta type entities."""

    code: str = Field(..., alias="Code")
    id_type: int = Field(..., alias="IdType")
    sql_table_name: str = Field(..., alias="SqlTableName")

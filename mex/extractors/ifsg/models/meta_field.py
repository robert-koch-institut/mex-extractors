from pydantic import Field

from mex.common.models import BaseModel


class MetaField(BaseModel):
    """Model class for ifsg meta field entities."""

    gui_text: str = Field(..., alias="GuiText")
    gui_tool_tip: str | None = Field(..., alias="GuiTooltip")
    id_catalogue: int = Field(..., alias="IdCatalogue")
    id_type: int = Field(..., alias="IdType")
    id_data_type: int = Field(..., alias="IdDataType")
    id_field: int = Field(..., alias="IdField")
    id_field_type: int = Field(..., alias="IdFieldType")
    to_transport: int = Field(..., alias="ToTransport")
    sort: int = Field(..., alias="Sort")
    statement_area_group: str | None = Field(..., alias="StatementAreaGroup")

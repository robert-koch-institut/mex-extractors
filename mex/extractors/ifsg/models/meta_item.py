from pydantic import Field

from mex.common.models import BaseModel


class MetaItem(BaseModel):
    """Model class for ifsg meta item entities."""

    item_name: str = Field(..., alias="ItemName")
    item_name_en: str | None = Field(..., alias="ItemNameEN", min_length=0)
    id_item: int = Field(..., alias="IdItem")

from pydantic import Field

from mex.common.models import BaseModel


class MetaCatalogue2Item(BaseModel):
    """Model class for ifsg catalogue2item entities."""

    id_item: int = Field(..., alias="IdItem")
    id_catalogue: int = Field(..., alias="IdCatalogue")
    id_catalogue2item: int = Field(..., alias="IdCatalogue2Item")

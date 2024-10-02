from pydantic import Field

from mex.common.models import BaseModel


class MetaCatalogue2Item2Schema(BaseModel):
    """Model class for ifsg catalogue2item2schema entities."""

    id_catalogue2item: int = Field(..., alias="IdCatalogue2Item")

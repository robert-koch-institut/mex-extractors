from pydantic import Field

from mex.common.models import BaseModel


class DatenkompassSettings(BaseModel):
    """Settings submodel for the datenkompass extractor."""

    unit_filter: str = Field("e.g. unit", description="Filter for unit")
    organization_filter: str = Field(
        "Organization", description="Filter for organization"
    )

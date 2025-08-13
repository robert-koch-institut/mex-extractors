from pydantic import Field

from mex.common.models import BaseModel


class DatenkompassSettings(BaseModel):
    """Settings submodel for the datenkompass extractor."""

    cutoff_number_authors: int = Field(
        3,
        description="Maximum number of extracted authors for Bibliographic resources",
    )

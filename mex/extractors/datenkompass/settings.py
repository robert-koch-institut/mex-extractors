from pydantic import Field

from mex.common.models import BaseModel


class DatenkompassSettings(BaseModel, str_strip_whitespace=False):
    """Settings submodel for the datenkompass extractor."""

    unit_filter: str = Field("e.g. unit", description="Filter for unit")
    organization_filter: str = Field(
        "Organization", description="Filter for organization"
    )
    cutoff_number_authors: int = Field(
        3,
        description="Maximum number of extracted authors for Bibliographic resources",
    )
    list_delimiter: str = Field(
        "; ",
        description="Seperator for different entries in a datenkompass model field.",
    )

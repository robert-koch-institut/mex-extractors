from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class DatenkompassSettings(BaseModel, str_strip_whitespace=False):
    """Settings submodel for the datenkompass extractor."""

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
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/mapping-to-external-schema/datenkompass"),
        description=(
            "Path to the directory with the datenkompass mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

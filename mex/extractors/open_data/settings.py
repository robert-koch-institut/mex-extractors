from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class OpenDataSettings(BaseModel):
    """Zenodo settings submodel definition for the Open Data extractor."""

    url: str = Field("https://zenodo", description="Zenodo instance URL")
    community_rki: str = Field(
        "robertkochinstitut", description="Zenodo communitiy of rki"
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/open-data"),
        description=(
            "Path to the directory with the open data mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

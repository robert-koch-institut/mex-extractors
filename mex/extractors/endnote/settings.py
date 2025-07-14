from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class EndnoteSettings(BaseModel):
    """Settings submodel for the endnote extractor."""

    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/endnote"),
        description=(
            "Path to the directory with the endnote mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

    cutoff_number_authors: int = 42

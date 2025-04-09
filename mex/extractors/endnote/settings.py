from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class EndnoteSettings(BaseModel):
    """Settings submodel for the endnote extractor."""

    raw_data_path: AssetsPath = Field(
        AssetsPath("raw-data/endnote"),
        description=(
            "Path to the directory with the endnote xml files, "
            "absolute path or relative to `assets_dir`."
        ),
    )

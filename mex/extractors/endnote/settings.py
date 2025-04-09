from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class EndnoteSettings(BaseModel):
    """Settings submodel for the Biospecimen extractor."""

    raw_data_path: AssetsPath = Field(
        AssetsPath("raw-data/endnote"),
        description=(
            "Path to the directory with the biospecimen excel files, "
            "absolute path or relative to `assets_dir`."
        ),
    )

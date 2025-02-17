from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class VoxcoSettings(BaseModel):
    """Settings submodel for the Voxco extractor."""

    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/voxco"),
        description=(
            "Path to the directory with the voxco mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

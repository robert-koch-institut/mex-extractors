from pydantic import Field

from mex.common.models import BaseModel


class VoxcoSettings(BaseModel):
    """Settings submodel for the Voxco extractor."""

    mapping_path: str = Field(
        "mappings/voxco",
        description=(
            "Path to the directory with the voxco mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

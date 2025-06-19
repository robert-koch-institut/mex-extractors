from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class IGSSettings(BaseModel):
    """IGS settings submodel definition for the IGS extractor."""

    url: str = Field("https://igs", description="URL of IGS instance")
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/igs"),
        description=(
            "Path to the directory with the igs mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

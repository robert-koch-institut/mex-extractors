from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class SumoSettings(BaseModel):
    """Settings submodel for the SUMO extractor."""

    raw_data_path: AssetsPath = Field(
        AssetsPath("raw-data/sumo"),
        description=(
            "Path to the directory with the sumo excel files, "
            "absolute path or relative to `assets_dir`."
        ),
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/sumo"),
        description=(
            "Path to the directory with the sumo mapping files containing the default "
            "values, absolute path or relative to `assets_dir`."
        ),
    )

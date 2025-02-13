from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class ODKSettings(BaseModel):
    """Settings submodel definition for odk data extraction."""

    raw_data_path: AssetsPath = Field(
        AssetsPath("raw-data/odk"),
        description=(
            "Path to the directory with the odk excel files, "
            "absolute path or relative to `assets_dir`."
        ),
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/odk"),
        description=(
            "Path to the directory with the odk mapping files containing the default "
            "values, absolute path or relative to `assets_dir`."
        ),
    )

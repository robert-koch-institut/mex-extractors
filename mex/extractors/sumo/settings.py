from pydantic import Field

from mex.common.models import BaseModel


class SumoSettings(BaseModel):
    """Settings submodel for the SUMO extractor."""

    raw_data_path: str = Field(
        "raw-data/sumo",
        description=(
            "Path to the directory with the sumo excel files, "
            " relative to `assets_dir`."
        ),
    )
    mapping_path: str = Field(
        "mappings/sumo",
        description=(
            "Path to the directory with the sumo mapping files containing the default "
            "values, relative to `assets_dir`."
        ),
    )

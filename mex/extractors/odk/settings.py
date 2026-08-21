from pydantic import Field

from mex.common.models import BaseModel


class ODKSettings(BaseModel):
    """Settings submodel definition for odk data extraction."""

    raw_data_path: str = Field(
        "raw-data/odk",
        description=(
            "Path to the directory with the odk excel files, "
            "absolute path or relative to `assets_dir`."
        ),
    )
    mapping_path: str = Field(
        "mappings/odk",
        description=(
            "Path to the directory with the odk mapping files containing the default "
            "values, absolute path or relative to `assets_dir`."
        ),
    )

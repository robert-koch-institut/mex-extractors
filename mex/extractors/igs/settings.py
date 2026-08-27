from pydantic import Field

from mex.common.models import BaseModel


class IGSSettings(BaseModel):
    """IGS settings submodel definition for the IGS extractor."""

    url: str = Field("https://igs", description="URL of IGS instance")
    mapping_path: str = Field(
        "mappings/igs",
        description=(
            "Path to the directory with the IGS mapping files containing the "
            "default values, relative to `assets_dir`."
        ),
    )

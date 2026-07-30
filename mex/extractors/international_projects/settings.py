from pydantic import Field

from mex.common.models import BaseModel


class InternationalProjectsSettings(BaseModel):
    """Settings submodel definition for the international projects extractor."""

    file_path: str = Field(
        "raw-data/international-projects/international_projects.xlsx",
        description=(
            "Path to the international projects excel file, "
            "absolute path or relative to `assets_dir`."
        ),
    )
    mapping_path: str = Field(
        "mappings/international-projects",
        description=(
            "Path to the directory with the international-projects mapping files "
            "containing the default values, absolute path or relative to `assets_dir`."
        ),
    )

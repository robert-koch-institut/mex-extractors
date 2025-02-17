from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class FFProjectsSettings(BaseModel):
    """Settings submodel for the FF Projects extractor."""

    file_path: AssetsPath = Field(
        AssetsPath("raw-data/ff-projects/ff-projects.xlsx"),
        description=(
            "Path to the FF Projects excel file, "
            "absolute path or relative to `assets_dir`."
        ),
    )
    skip_funding: list[str] = Field(
        ["Sonstige"], description="Skip sources with this funding"
    )
    skip_topics: list[str] = Field(
        ["Sonstige"],
        description="Skip sources with these topics",
    )
    skip_years_strings: list[str] = Field(
        ["fehlt", "keine", "offen"],
        description="Skip sources with these years",
    )
    skip_clients: list[str] = Field(
        ["Sonstige"],
        description="Skip sources with these clients",
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/ff-projects"),
        description=(
            "Path to the directory with the ff-projects mapping files"
            "values, absolute path or relative to `assets_dir`."
        ),
    )

from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class PublisherSettings(BaseModel):
    """Settings submodel definition for the publishing pipeline."""

    skip_entity_types: list[str] = Field(
        ["MergedPrimarySource", "MergedConsent"],
        description="Skip publishing items with these types.",
    )
    allowed_person_primary_sources: list[str] = Field(
        ["endnote"],
        description="Allow persons from these primary sources to be published.",
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings"),
        description=(
            "Path to the directory with the filter mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

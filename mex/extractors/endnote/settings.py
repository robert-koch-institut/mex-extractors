from pydantic import Field

from mex.common.models import BaseModel


class EndnoteSettings(BaseModel):
    """Settings submodel for the endnote extractor."""

    mapping_path: str = Field(
        "mappings/endnote",
        description=(
            "Path to the directory with the endnote mapping files containing the "
            "default values, relative to `assets_dir`."
        ),
    )

    cutoff_number_authors: int = Field(
        42, description="maximum number of authors to extract per publication"
    )

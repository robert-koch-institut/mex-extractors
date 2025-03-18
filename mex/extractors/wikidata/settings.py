from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class WikidataSettings(BaseModel):
    """Wikidata settings submodel definition for the Wikidata extractor."""

    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/wikidata"),
        description=(
            "Path to the directory with the wikidata mapping files"
            "values, absolute path or relative to `assets_dir`."
        ),
    )

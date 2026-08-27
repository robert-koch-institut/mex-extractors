from pydantic import Field

from mex.common.models import BaseModel


class WikidataSettings(BaseModel):
    """Wikidata settings submodel definition for the Wikidata extractor."""

    mapping_path: str = Field(
        "mappings/wikidata",
        description=(
            "Path to the directory with the wikidata mapping files"
            "values, relative to `assets_dir`."
        ),
    )

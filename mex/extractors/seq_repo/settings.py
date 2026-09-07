from pydantic import Field

from mex.common.models import BaseModel


class SeqRepoSettings(BaseModel):
    """Settings submodel for the SeqRepo extractor."""

    fallback_unit: str = Field(
        "C1",
        description=("Default unit if unit can't be extracted otherwise."),
    )
    mapping_path: str = Field(
        "mappings/seq-repo",
        description=(
            "Path to the directory with the seq-repo mapping files containing the "
            "default values, relative to `assets_dir`."
        ),
    )

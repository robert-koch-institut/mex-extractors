from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class SeqRepoSettings(BaseModel):
    """Settings submodel for the SeqRepo extractor."""

    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/seq-repo"),
        description=(
            "Path to the directory with the seq-repo mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

from pydantic import Field, SecretStr

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class BlueAntSettings(BaseModel):
    """Blue Ant settings submodel definition for the Blue Ant extractor."""

    api_key: SecretStr = Field(
        SecretStr("api-key"),
        description="Json Web Token for authentication with the Blue Ant API",
    )
    url: str = Field("https://blueant", description="URL of Blue Ant instance")
    skip_labels: list[str] = Field(
        ["test"], description="Skip projects with these terms in their label"
    )
    delete_prefixes: list[str] = Field(
        ["_", "1_", "2_", "3_", "4_", "5_", "6_", "7_", "8_", "9_"],
        description="Delete prefixes of labels starting with these terms",
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/blueant"),
        description=(
            "Path to the directory with the blueant mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

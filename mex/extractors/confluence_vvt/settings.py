from pydantic import Field, SecretStr

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class ConfluenceVvtSettings(BaseModel):
    """Confluence-vvt settings submodule definition for the Confluence-vvt extractor."""

    url: str = Field("https://confluence.vvt", description="URL of Confluence-vvt.")
    username: SecretStr = Field(
        SecretStr("username"),
        description="Confluence-vvt user name",
    )
    password: SecretStr = Field(
        SecretStr("password"),
        description="Confluence-vvt password",
    )
    overview_page_id: str = Field(
        "123456", description="Confluence id of the overview page."
    )
    template_v1_mapping_path: AssetsPath = Field(
        AssetsPath("mappings/confluence-vvt_template_v1"),
        description=(
            "Path to the directory with the confluence-vvt mapping files containing the"
            " default values, absolute path or relative to `assets_dir`."
        ),
    )
    skip_pages: list[str] = Field(
        ["123456"],
        description=(
            "List of Confluence-vvt page ids that must be skipped for incomplete "
            "or broken data, otherwise it will break the extractor."
        ),
    )

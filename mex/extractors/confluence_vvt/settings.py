from pydantic import Field, SecretStr

from mex.common.models import BaseModel


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

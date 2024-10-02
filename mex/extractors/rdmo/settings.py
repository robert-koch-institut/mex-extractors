from pydantic import Field, SecretStr

from mex.common.models import BaseModel


class RDMOSettings(BaseModel):
    """RDMO settings submodel definition for the RDMO extractor."""

    url: str = Field("https://rdmo/", description="RDMO instance URL")
    username: SecretStr = Field(
        SecretStr("username"),
        description="RDMO API user name",
    )
    password: SecretStr = Field(
        SecretStr("password"),
        description="RDMO API password",
    )

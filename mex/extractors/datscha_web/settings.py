from pydantic import Field, SecretStr

from mex.common.models import BaseModel


class DatschaWebSettings(BaseModel):
    """Settings submodel definition for datscha web extractor."""

    url: str = Field("https://datscha/", description="URL of datscha web service.")
    vorname: SecretStr = Field(
        SecretStr("first-name"),
        description="First name for login to datscha web service.",
    )
    nachname: SecretStr = Field(
        SecretStr("last-name"),
        description="Last name for login to datscha web service.",
    )
    pw: SecretStr = Field(
        SecretStr("password"),
        description="Password for login to datscha web service.",
    )
    organisation: str = Field(
        "RKI",
        description="Organisation for login to datscha web service.",
    )

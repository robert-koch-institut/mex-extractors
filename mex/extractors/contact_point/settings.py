from pydantic import Field

from mex.common.models import BaseModel


class ContactPointSettings(BaseModel):
    """Settings submodel definition for contact point extractor."""

    mex_email: str = Field(
        "mex@rki.de",
        description="Default email address.",
    )

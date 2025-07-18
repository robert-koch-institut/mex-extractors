from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import Email


class ContactPointSettings(BaseModel):
    """Settings submodel definition for contact point extractor."""

    mex_email: Email = Field(
        Email("mex@rki.de"),
        description="Default email address.",
    )

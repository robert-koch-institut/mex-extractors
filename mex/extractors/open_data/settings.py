from pydantic import Field

from mex.common.models import BaseModel


class OpenDataSettings(BaseModel):
    """Zenodo settings submodel definition for the Open Data extractor."""

    url: str = Field("https://zenodo", description="Zenodo instance URL")

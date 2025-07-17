from pydantic import Field

from mex.common.models import BaseModel


class PublisherSettings(BaseModel):
    """Settings submodel definition for the publishing pipeline."""

    skip_merged_items: list[str] = Field(
        ["MergedPrimarySource", "MergedConsent", "MergedPerson"],
        description="Skip merged items with these types during publishing.",
    )
    allowed_person_primary_sources: list[str] = Field(
        ["endnote"],
        description="Allow persons from these primary sources to be published.",
    )

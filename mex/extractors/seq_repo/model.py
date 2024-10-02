from pydantic import Field

from mex.common.models import BaseModel


class SeqRepoSource(BaseModel):
    """Model class for Seq Repo Source."""

    project_coordinators: list[str] = Field(alias="project-coordinators")
    customer_org_unit_id: str = Field(alias="customer-org-unit-id")
    sequencing_date: str = Field(alias="sequencing-date")
    lims_sample_id: str = Field(alias="lims-sample-id")
    sequencing_platform: str = Field(alias="sequencing-platform")
    species: str
    project_name: str = Field(alias="project-name")
    customer_sample_name: str = Field(alias="customer-sample-name")
    project_id: str = Field(alias="project-id")

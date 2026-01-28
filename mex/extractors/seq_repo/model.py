from collections.abc import Sequence

from pydantic import Field

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData


class SeqRepoSource(BaseRawData):
    """Model class for Seq Repo Source."""

    project_coordinators: list[str] = Field(alias="project-coordinators")
    customer_org_unit_id: str | None = Field(None, alias="customer-org-unit-id")
    sequencing_date: str = Field(alias="sequencing-date")
    lims_sample_id: str = Field(alias="lims-sample-id")
    sequencing_platform: str = Field(alias="sequencing-platform")
    species: str
    project_name: str = Field(alias="project-name")
    customer_sample_name: str = Field(alias="customer-sample-name")
    project_id: str = Field(alias="project-id")

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return []

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return None

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return None

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return self.project_coordinators

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return self.project_id

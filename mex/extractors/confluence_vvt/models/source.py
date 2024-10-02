from collections.abc import Sequence
from typing import cast

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData


class ConfluenceVvtSource(BaseRawData):
    """Model class for Confluence-vvt sources."""

    abstract: str | None
    activity_type: str | None = None
    alternative_title: str | None = None
    contact: list[str] = []
    documentation: str | None = None
    end: str | None = None
    gemeinsam_verantwortliche: str | None = None
    funder_or_commissioner: str | None = None
    funding_program: str | None = None
    identifier: str
    identifier_in_primary_source: list[str]
    involved_person: list[str] = []
    involved_unit: list[str] = []
    is_part_of_activity: str | None = None
    publication: str | None = None
    responsible_unit: list[str]
    short_name: str | None = None
    start: str | None = None
    succeeds: str | None = None
    theme: str
    title: str | None = None
    website: str | None = None

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
        return cast(list[str | None], self.contact)

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return None

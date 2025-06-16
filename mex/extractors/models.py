from abc import abstractmethod
from collections.abc import Sequence

from mex.common.models import BaseModel
from mex.common.types import TemporalEntity


class BaseRawData(BaseModel):
    """Raw-data base providing standardized access to attributes for filtering."""

    @abstractmethod
    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""

    @abstractmethod
    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""

    @abstractmethod
    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""

    @abstractmethod
    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""

    @abstractmethod
    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""

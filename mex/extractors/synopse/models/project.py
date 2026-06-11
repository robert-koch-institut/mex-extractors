from typing import TYPE_CHECKING

from pydantic import ConfigDict, Field

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData

if TYPE_CHECKING:
    from collections.abc import Sequence


class ProjektUndStudienverwaltung(BaseRawData):
    """Model for synopse projects with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    Studie: str | None = None
    ProjektStudientitel: str = Field(...)
    BeschreibungStudie: str | None = None
    StudienArtTyp: str | None = None
    Projektbeginn: str | None = Field(None, ge="1891")
    Projektende: str | None = Field(None, ge="1891")
    Beitragende: str | None = None
    Partner_intern: str | None = None
    VerantwortlicheOE: str | None = None
    Partner_extern: str | None = None
    Auftraggeber: str | None = None
    Anschlussprojekt: str | None = None
    Projektdokumentation: str | None = None
    StudienID: str = Field(...)


    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return [self.Partner_intern]

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return TemporalEntity(self.Projektbeginn) if self.Projektbeginn else None

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return TemporalEntity(self.Projektende) if self.Projektende else None

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [self.VerantwortlicheOE]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return self.StudienID

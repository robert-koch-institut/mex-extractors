from typing import TYPE_CHECKING

from pydantic import ConfigDict, Field

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData

if TYPE_CHECKING:
    from collections.abc import Sequence


class ProjektUndStudienverwaltung(BaseRawData):
    """Model for synopse projects with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    studien_id: str = Field(..., alias="StudienID")
    studie: str | None = Field(None, alias="Studie")
    studien_art_typ: str | None = Field(None, alias="StudienArtTyp")
    projekt_studientitel: str | None = Field(None, alias="ProjektStudientitel")
    beschreibung_studie: str | None = Field(None, alias="BeschreibungStudie")
    beitragende: str | None = Field(None, alias="Beitragende")
    verantwortliche_oe: str | None = Field(None, alias="VerantwortlicheOE")
    partner_intern: str | None = Field(None, alias="Partner_intern")
    partner_extern: str | None = Field(None, alias="Partner_extern")
    projektbeginn: str | None = Field(None, alias="Projektbeginn", ge="1891")
    projektende: str | None = Field(None, alias="Projektende", ge="1891")
    auftraggeber: str | None = Field(None, alias="Auftraggeber")
    projektdokumentation: str | None = Field(None, alias="Projektdokumentation")
    anschlussprojekt: str | None = Field(None, alias="Anschlussprojekt")

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return [self.partner_intern]

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return TemporalEntity(self.projektbeginn) if self.projektbeginn else None

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return TemporalEntity(self.projektende) if self.projektende else None

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [self.verantwortliche_oe]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return self.studien_id

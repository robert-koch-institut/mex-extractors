from typing import TYPE_CHECKING

from pydantic import ConfigDict, Field

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData

if TYPE_CHECKING:
    from collections.abc import Sequence


class SynopseProject(BaseRawData):
    """Model for synopse projects with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    akronym_des_studientitels: str | None = Field(None, alias="Studie")
    project_studientitel: str | None = Field(..., alias="ProjektStudientitel")
    beschreibung_der_studie: str | None = Field(None, alias="BeschreibungStudie")
    studienart_studientyp: str | None = Field(None, alias="StudienArtTyp")
    projektbeginn: str | None = Field(None, ge="1891", alias="Projektbeginn")
    projektende: str | None = Field(None, ge="1891", alias="Projektende")
    beitragende: str | None = Field(None, alias="Beitragende")
    interne_partner: str | None = Field(None, alias="Partner_intern")
    verantwortliche_oe: str | None = Field(None, alias="VerantwortlicheOE")
    externe_partner: str | None = Field(None, alias="Partner_extern")
    foerderinstitution_oder_auftraggeber: str | None = Field(None, alias="Auftraggeber")
    anschlussprojekt: str | None = Field(None, alias="Anschlussprojekt")
    projektdokumentation: str | None = Field(None, alias="Projektdokumentation")
    studien_id: str = Field(..., alias="StudienID")

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return [self.interne_partner]

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return TemporalEntity(self.projektende) if self.projektende else None

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return TemporalEntity(self.projektbeginn) if self.projektbeginn else None

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [self.verantwortliche_oe]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return self.studien_id

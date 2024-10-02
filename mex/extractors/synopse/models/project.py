from pydantic import ConfigDict, Field

from mex.common.models import BaseModel


class SynopseProject(BaseModel):
    """Model for synopse projects with aliases to match CSV columns."""

    model_config = ConfigDict(extra="ignore")

    akronym_des_studientitels: str = Field(..., alias="Studie")
    project_studientitel: str | None = Field(None, alias="ProjektStudientitel")
    beschreibung_der_studie: str | None = Field(None, alias="BeschreibungStudie")
    studienart_studientyp: str = Field(..., alias="StudienArtTyp")
    projektbeginn: int | None = Field(None, ge=1891, alias="Projektbeginn")
    projektende: int | None = Field(None, ge=1891, alias="Projektende")
    kontakt: list[str] = Field([], alias="Kontakt")
    beitragende: str | None = Field(None, alias="Beitragende")
    interne_partner: str | None = Field(None, alias="Partner_intern")
    verantwortliche_oe: str | None = Field(None, alias="VerantwortlicheOE")
    externe_partner: str | None = Field(None, alias="Partner_extern")
    foerderinstitution_oder_auftraggeber: str | None = Field(None, alias="Auftraggeber")
    anschlussprojekt: str | None = Field(None, alias="Anschlussprojekt")
    projektdokumentation: str | None = Field(None, alias="Projektdokumentation")
    studien_id: str = Field(..., alias="StudienID")

    def get_contacts(self) -> list[str]:
        """Split the values for the `kontakt` field by comma into a list."""
        return [
            value.strip().lower()
            for kontakt in self.kontakt
            for value in kontakt.split(",")
        ]

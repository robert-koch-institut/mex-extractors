from collections.abc import Sequence

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData


class FFProjectsSource(BaseRawData):
    """Model class for FF Projects source entities."""

    kategorie: str | None
    foerderprogr: str | None
    thema_des_projekts: str
    rki_az: str
    laufzeit_cells: tuple[str | None, str | None]
    laufzeit_bis: TemporalEntity | None = None
    laufzeit_von: TemporalEntity | None = None
    projektleiter: str
    rki_oe: str | None = None
    zuwendungs_oder_auftraggeber: str
    lfd_nr: str

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return []

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return self.laufzeit_von

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return self.laufzeit_bis

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [self.rki_oe]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return self.lfd_nr

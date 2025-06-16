from collections.abc import Sequence

from pydantic import Field

from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData


class DatschaWebItem(BaseRawData):
    """Model class for metadata items coming from datscha web."""

    item_id: int
    bezeichnung_der_verarbeitungstaetigkeit: str = Field(
        ..., alias="Bezeichnung der Verarbeitungstätigkeit"
    )
    version: str = Field(..., alias="Version")
    kurzbeschreibung: str | None = Field(None, alias="Kurzbeschreibung")
    zentrale_stelle_fuer_die_verarbeitung: str | None = Field(
        None, alias="Zentrale Stelle für die Verarbeitung"
    )
    zweckbestimmung_der_datenverarbeitung: str | None = Field(
        None, alias="Zweckbestimmung der Datenverarbeitung"
    )
    rechtsgrundlage: str | None = Field(None, alias="Rechtsgrundlage")
    herkunft_der_daten: str | None = Field(None, alias="Herkunft der Daten")
    betroffene_personen: str | None = Field(None, alias="Betroffene Personen")
    daten_oder_datenkategorien: str | None = Field(
        None, alias="Daten oder Datenkategorien"
    )
    loeschfrist_der_daten: str | None = Field(None, alias="Löschfrist der Daten")
    zugriffsberechtigte: str | None = Field(None, alias="Zugriffsberechtigte")
    gemeinsames_oder_verbundenes_verfahren: str | None = Field(
        None, alias="Gemeinsames/Verbundenes Verfahren"
    )
    abrufverfahren: str | None = Field(None, alias="Abrufverfahren")
    verantwortlicher_fuer_das_abrufverfahren: str | None = Field(
        None,
        alias="Verantwortlicher für das Abrufverfahren",
    )
    eu_verfahren: str | None = Field(None, alias="EU-Verfahren")
    auftragsverarbeitung_art_28_dsgvo: str | None = Field(
        None, alias="Auftragsverarbeitung (Art.\xa028\xa0DSGVO)"
    )
    auftragsverarbeiter: str | None = Field(None, alias="Auftragsverarbeiter")
    datenverarbeitung_uebermittlung_oder_offenlegung: str | None = Field(
        None, alias="Datenverarbeitung (Übermittlung/Offenlegung)"
    )
    art_der_verarbeiteten_uebermittelten_oder_offengelegten_daten: str | None = Field(
        None, alias="Art der verarbeiteten (übermittelten/offengelegten) Daten"
    )
    empfaenger_der_verarbeiteten_uebermittelten_oder_offengelegten_daten: str | None = (
        Field(
            None,
            alias="Empfänger der verarbeiteten (übermittelten/offengelegten) Daten",
        )
    )
    datenuebermittlung_in_drittstaat: str | None = Field(
        None, alias="Datenübermittlung in Drittstaat"
    )
    name_des_drittstaats: str | None = Field(None, alias="Name des Drittstaats")
    art_der_verarbeiteten_uebermittelten_oder_offengelegten_daten_in_drittstaat: (
        str | None
    ) = Field(
        None,
        alias="Art der verarbeiteten (übermittelten/offengelegten) Daten in Drittstaat",
    )
    empfaenger_der_daten_im_drittstaat: str | None = Field(
        None, alias="Empfänger der Daten im Drittstaat"
    )
    hard_und_software: str | None = Field(None, alias="Hard- und Software")
    schutzmassnahmen_art_32_dsgvo: str | None = Field(
        None, alias="Schutzmaßnahmen (Art.\xa032\xa0DSGVO)"
    )
    zugriffskontrolle: str | None = Field(None, alias="Zugriffskontrolle")
    weitergabekontrolle: str | None = Field(None, alias="Weitergabekontrolle")
    eingabekontrolle: str | None = Field(None, alias="Eingabekontrolle")
    auftragskontrolle: str | None = Field(None, alias="Auftragskontrolle")
    verfuegbarkeitskontrolle: str | None = Field(None, alias="Verfügbarkeitskontrolle")
    trennungsgebot: str | None = Field(None, alias="Trennungsgebot")
    datenschutz_folgenabschaetzung_art_35_dsgvo: str | None = Field(
        None, alias="Datenschutz-Folgenabschätzung (Art.\xa035\xa0DSGVO)"
    )
    besteht_die_verarbeitung_aus_mehreren_dateien_oder_datenbanken: str | None = Field(
        None, alias="Besteht die Verarbeitung aus mehreren Dateien/Datenbanken"
    )
    beschreibung_der_verarbeitungstaetigkeit_dateien: str | None = Field(
        None, alias="Beschreibung der Verarbeitungstätigkeit (Dateien)"
    )
    auskunftsperson: str | None = Field(None, alias="Auskunftsperson")
    bemerkungen: str | None = Field(None, alias="Bemerkungen")
    aufnahme_oder_aenderungsdatum: str | None = Field(
        None, alias="Aufnahme-/Änderungsdatum"
    )
    erfassername: str | None = Field(None, alias="Erfassername")
    liegenschaften_oder_organisationseinheiten_loz: list[str] = Field(
        [], alias="Liegenschaften/Organisationseinheiten (LOZ)"
    )

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return [
            partner
            for partner in (
                self.auftragsverarbeiter,
                self.empfaenger_der_daten_im_drittstaat,
                self.empfaenger_der_verarbeiteten_uebermittelten_oder_offengelegten_daten,
            )
            if partner
        ]

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return None

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return None

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [
            unit
            for unit in (
                *self.liegenschaften_oder_organisationseinheiten_loz,
                self.auskunftsperson,
                self.zentrale_stelle_fuer_die_verarbeitung,
            )
            if unit
        ]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return str(self.item_id)

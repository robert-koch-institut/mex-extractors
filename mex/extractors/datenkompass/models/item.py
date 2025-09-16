from pydantic import BaseModel, Field

from mex.common.types import (
    MergedIdentifier,
)


class DatenkompassActivity(BaseModel):
    """Model for Datenkompass Activities."""

    model_config = {
        "populate_by_name": True  # allows using field names instead of aliases
    }

    beschreibung: str = Field(..., alias="Beschreibung")
    datenhalter: str = Field(
        ..., alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    kontakt: str | None = Field(None, alias="Kontakt (Herausgeber)")
    organisationseinheit: str = Field(..., alias="Organisationseinheit")
    titel: str = Field(..., alias="Titel")
    schlagwort: str | None = Field(None, alias="Schlagwort")
    datenbank: str | None = Field(None, alias="Link oder Datenbank")
    voraussetzungen: str = Field(
        ..., alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    frequenz: str = Field(..., alias="Frequenz der Aktualisierung")
    hauptkategorie: str = Field(..., alias="Hauptkategorie")
    unterkategorie: str = Field(..., alias="Unterkategorie")
    rechtsgrundlage: str = Field(..., alias="Rechtsgrundlage für die Zugangseröffnung")
    datenerhalt: str = Field(..., alias="Weg des Datenerhalts")
    status: str = Field(..., alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: str = Field(..., alias="Datennutzungszweck")
    herausgeber: str = Field(..., alias="Herausgeber")
    kommentar: str = Field(..., alias="Kommentar")
    format: str = Field(..., alias="Format der Daten")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


class DatenkompassBibliographicResource(BaseModel):
    """Model for Datenkompass Bibliographic Resources."""

    model_config = {"populate_by_name": True}

    beschreibung: str | None = Field(None, alias="Beschreibung")
    kontakt: str | None = Field(None, alias="Kontakt (Herausgeber)")
    organisationseinheit: str | None = Field(None, alias="Organisationseinheit")
    titel: str = Field(..., alias="Titel")
    datenhalter: str = Field(
        ..., alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    dk_format: str = Field(..., alias="Format der Daten")
    frequenz: str = Field(..., alias="Frequenz der Aktualisierung")
    schlagwort: str | None = Field(None, alias="Schlagwort")
    datenbank: str | None = Field(None, alias="Link oder Datenbank")
    rechtsgrundlagen_benennung: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung (Benennung)"
    )
    datennutzungszweck_erweitert: str | None = Field(
        None, alias="Datennutzungszweck (erweitert)"
    )
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    hauptkategorie: str = Field(..., alias="Hauptkategorie")
    unterkategorie: str = Field(..., alias="Unterkategorie")
    datenerhalt: str = Field(..., alias="Weg des Datenerhalts")
    status: str = Field(..., alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: str = Field(..., alias="Datennutzungszweck")
    rechtsgrundlage: str = Field(..., alias="Rechtsgrundlage für die Zugangseröffnung")
    herausgeber: str = Field(..., alias="Herausgeber")
    kommentar: str = Field(..., alias="Kommentar")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


class DatenkompassResource(BaseModel):
    """Model for Datenkompass Resources."""

    model_config = {"populate_by_name": True}

    beschreibung: str = Field(..., alias="Beschreibung")
    datenhalter: str = Field(
        ..., alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    frequenz: str | None = Field(None, alias="Frequenz der Aktualisierung")
    kontakt: str | None = Field(None, alias="Kontakt (Herausgeber)")
    organisationseinheit: str = Field(..., alias="Organisationseinheit")
    titel: str = Field(..., alias="Titel")
    schlagwort: str = Field(..., alias="Schlagwort")
    voraussetzungen: str = Field(
        ..., alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    datenbank: str | None = Field(None, alias="Link oder Datenbank")
    rechtsgrundlagen_benennung: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung (Benennung)"
    )
    datennutzungszweck_erweitert: str | None = Field(
        None, alias="Datennutzungszweck (erweitert)"
    )
    dk_format: str | None = Field(None, alias="Format der Daten")
    hauptkategorie: str = Field(..., alias="Hauptkategorie")
    unterkategorie: str = Field(..., alias="Unterkategorie")
    rechtsgrundlage: str = Field(..., alias="Rechtsgrundlage für die Zugangseröffnung")
    datenerhalt: str = Field(..., alias="Weg des Datenerhalts")
    status: str = Field(..., alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: str = Field(..., alias="Datennutzungszweck")
    herausgeber: str = Field(..., alias="Herausgeber")
    kommentar: str = Field(..., alias="Kommentar")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


AnyDatenkompassModel = (
    DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource
)

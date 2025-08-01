from pydantic import BaseModel, Field

from mex.common.types import (
    MergedIdentifier,
)


class DatenkompassActivity(BaseModel):
    """Model for Datenkompass Activities."""

    model_config = {
        "populate_by_name": True  # allows using field names instead of aliases
    }

    beschreibung: str | None = Field(None, alias="Beschreibung")
    datenhalter: str = Field(
        ..., alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    kontakt: list[str] = Field([], alias="Kontakt (Herausgeber)")
    titel: list[str] = Field([], alias="Titel")
    schlagwort: list[str] = Field([], alias="Schlagwort")
    datenbank: list[str] = Field([], alias="Link oder Datenbank")
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: str | None = Field(None, alias="Unterkategorie")
    rechtsgrundlage: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung"
    )
    datenerhalt: str | None = Field(None, alias="Weg des Datenerhalts")
    status: str | None = Field(None, alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: str | None = Field(None, alias="Datennutzungszweck")
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    format: str | None = Field(None, alias="Format")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


class DatenkompassBibliographicResource(BaseModel):
    """Model for Datenkompass Bibliographic Resources."""

    model_config = {"populate_by_name": True}

    beschreibung: list[str] = Field([], alias="Beschreibung")
    kontakt: list[str] = Field([], alias="Kontakt (Herausgeber)")
    titel: str | None = Field(None, alias="Titel")
    schlagwort: list[str] = Field([], alias="Schlagwort")
    datenbank: str | None = Field(None, alias="Link oder Datenbank")
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: str | None = Field(None, alias="Unterkategorie")
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    dk_format: list[str] = Field([], alias="Format")  # "format" would shadow a builtIn
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


class DatenkompassResource(BaseModel):
    """Model for Datenkompass Resources."""

    model_config = {"populate_by_name": True}

    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    frequenz: list[str] = Field([], alias="Frequenz der Aktualisierung")
    kontakt: list[str] = Field([], alias="Kontakt (Herausgeber)")
    beschreibung: str | None = Field(None, alias="Beschreibung")
    datenbank: str | None = Field(None, alias="Link oder Datenbank")
    rechtsgrundlagenbenennung: list[str] = Field(
        [], alias="Rechtsgrundlage für die Zugangseröffnung (Benennung)"
    )
    datennutzungszweckerweitert: list[str] = Field(
        [], alias="Datennutzungszweck (erweitert)"
    )
    schlagwort: list[str] = Field([], alias="Schlagwort")
    dk_format: list[str] = Field(  # "format" would shadow a builtIn
        [], alias="Format"
    )
    titel: list[str] = Field([], alias="Titel")
    datenhalter: str | None = Field(
        None, alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: list[str] = Field([], alias="Unterkategorie")
    rechtsgrundlage: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung"
    )
    datenerhalt: str | None = Field(None, alias="Weg des Datenerhalts")
    status: str | None = Field(None, alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: list[str] = Field([], alias="Datennutzungszweck")
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


AnyDatenkompassModel = (
    DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource
)

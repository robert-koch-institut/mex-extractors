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
    kontakt: list[str] | None = Field(None, alias="Kontakt (Herausgeber)")
    titel: list[str] | None = Field(None, alias="Titel")
    schlagwort: list[str | None] = Field([], alias="Schlagwort")
    datenbank: list[str] | None = Field(None, alias="Link oder Datenbank")
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

    beschreibung: str | None = Field(None, alias="Beschreibung")
    kontakt: list[str] | None = Field(None, alias="Kontakt (Herausgeber)")
    titel: list[str] | None = Field(None, alias="Titel")
    schlagwort: list[str | None] = Field([], alias="Schlagwort")
    datenbank: list[str] | None = Field(None, alias="Link oder Datenbank")
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: str | None = Field(None, alias="Unterkategorie")
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    dk_format: str | None = Field(
        None, alias="Format"
    )  # "format" would shadow a builtIn
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


class DatenkompassResource(BaseModel):
    """Model for Datenkompass Resources."""

    model_config = {"populate_by_name": True}

    beschreibung: str | None = Field(None, alias="Beschreibung")
    datenhalter: str = Field(
        ..., alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    frequenz: str | None = Field(None, alias="Frequenz der Aktualisierung")
    kontakt: list[str] | None = Field(None, alias="Kontakt (Herausgeber)")
    titel: list[str] | None = Field(None, alias="Titel")
    schlagwort: list[str | None] = Field([], alias="Schlagwort")
    datenbank: list[str] | None = Field(None, alias="Link oder Datenbank")
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: str | None = Field(None, alias="Unterkategorie")
    rechtsgrundlage: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung (Benennung)"
    )
    datenerhalt: str | None = Field(None, alias="Weg des Datenerhalts")
    status: str | None = Field(None, alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: str | None = Field(None, alias="Datennutzungszweck (erweitert)")
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    format: str | None = Field(None, alias="Format")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: str = Field(exclude=True)  # ignore when writing to json


AnyDatenkompassModel = (
    DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource
)

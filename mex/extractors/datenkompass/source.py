from pydantic import BaseModel

from mex.common.types import (
    MergedIdentifier,
)


class DatenkompassActivity(BaseModel):
    """Model for target fields for Activities."""

    Beschreibung: str | None = None
    Halter: str | None = None
    Kontakt: list[str] | None = None
    Titel: list[str] | None = None
    Schlagwort: list[str | None]
    Datenbank: list[str] | None = None
    Voraussetzungen: str | None = None
    Hauptkategorie: str | None = None
    Unterkategorie: str | None = None
    Rechtsgrundlage: str | None = None
    Weg: str | None = None
    Status: str | None = None
    Datennutzungszweck: str | None = None
    Herausgeber: str | None = None
    Kommentar: str | None = None
    Format: str | None = None
    identifier: MergedIdentifier
    entityType: str


class DatenkompassBibliographicResource(BaseModel):
    """Model for target fields for Bibliographic Resources."""

    Beschreibung: str | None = None
    Voraussetzungen: str | None = None
    Datenbank: list[str] | None = None
    Format: str | None = None
    Kontakt: list[str] | None = None
    Schlagwort: list[str] | None = None
    Titel: list[str] | None = None
    Hauptkategorie: str | None = None
    Unterkategorie: str | None = None
    Herausgeber: str | None = None
    Kommentar: str | None = None
    identifier: MergedIdentifier
    entityType: str


class DatenkompassResource(BaseModel):
    """Model for target fields for Resources."""

    Voraussetzungen: str | None = None
    Frequenz: str | None = None
    Kontakt: list[str] | None = None
    Beschreibung: str | None = None
    Datenbank: list[str] | None = None
    Rechtsgrundlage: str | None = None
    Datennutzungszweck: str | None = None
    Format: str | None = None
    Schlagwort: list[str] | None = None
    Titel: list[str] | None = None
    Halter: str | None = None
    Hauptkategorie: str | None = None
    Unterkategorie: str | None = None
    Weg: str | None = None
    Status: str | None = None
    Herausgeber: str | None = None
    Kommentar: str | None = None
    identifier: MergedIdentifier
    entityType: str

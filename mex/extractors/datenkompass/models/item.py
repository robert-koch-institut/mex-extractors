from typing import Annotated, Literal

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
    kontakt: str | None = Field(None, alias="Kontakt (Herausgeber)")
    organisationseinheit: list[str] = Field([], alias="Organisationseinheit")
    titel: list[str] = Field([], alias="Titel")
    schlagwort: list[str] = Field([], alias="Schlagwort")
    datenbank: list[str] = Field([], alias="Link oder Datenbank")
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    frequenz: str | None = Field(None, alias="Frequenz der Aktualisierung")
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
    format: str | None = Field(None, alias="Format der Daten")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: Annotated[
        Literal["DatenkompassActivity"],
        Field(alias="$type", frozen=True, exclude=True),  # exclude when exporting
    ] = "DatenkompassActivity"


class DatenkompassBibliographicResource(BaseModel):
    """Model for Datenkompass Bibliographic Resources."""

    model_config = {"populate_by_name": True}

    beschreibung: str | None = Field(None, alias="Beschreibung")
    kontakt: str | None = Field(None, alias="Kontakt (Herausgeber)")
    organisationseinheit: list[str] = Field([], alias="Organisationseinheit")
    titel: str | None = Field(None, alias="Titel")
    datenhalter: str = Field(
        ..., alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    dk_format: str | None = Field(None, alias="Format der Daten")
    frequenz: str | None = Field(None, alias="Frequenz der Aktualisierung")
    schlagwort: list[str] = Field([], alias="Schlagwort")
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
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: str | None = Field(None, alias="Unterkategorie")
    datenerhalt: str | None = Field(None, alias="Weg des Datenerhalts")
    status: str | None = Field(None, alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: str | None = Field(None, alias="Datennutzungszweck")
    rechtsgrundlage: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung"
    )
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: Annotated[
        Literal["DatenkompassBibliographicResource"],
        Field(alias="$type", frozen=True, exclude=True),  # exclude when exporting
    ] = "DatenkompassBibliographicResource"


class DatenkompassResource(BaseModel):
    """Model for Datenkompass Resources."""

    model_config = {"populate_by_name": True}

    beschreibung: str | None = Field(None, alias="Beschreibung")
    datenhalter: str | None = Field(
        None, alias="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich"
    )
    frequenz: list[str] = Field([], alias="Frequenz der Aktualisierung")
    kontakt: str | None = Field(None, alias="Kontakt (Herausgeber)")
    organisationseinheit: list[str] = Field([], alias="Organisationseinheit")
    titel: list[str] = Field([], alias="Titel")
    schlagwort: list[str] = Field([], alias="Schlagwort")
    voraussetzungen: str | None = Field(
        None, alias="Formelle Voraussetzungen für den Datenerhalt"
    )
    datenbank: str | None = Field(None, alias="Link oder Datenbank")
    rechtsgrundlagen_benennung: list[str] = Field(
        [], alias="Rechtsgrundlage für die Zugangseröffnung (Benennung)"
    )
    datennutzungszweck_erweitert: list[str] = Field(
        [], alias="Datennutzungszweck (erweitert)"
    )
    dk_format: str | None = Field(  # "format" would shadow a builtIn
        None, alias="Format der Daten"
    )
    hauptkategorie: str | None = Field(None, alias="Hauptkategorie")
    unterkategorie: str | None = Field(None, alias="Unterkategorie")
    rechtsgrundlage: str | None = Field(
        None, alias="Rechtsgrundlage für die Zugangseröffnung"
    )
    datenerhalt: str | None = Field(None, alias="Weg des Datenerhalts")
    status: str | None = Field(None, alias="Status (planbare Verfügbarkeit der Daten)")
    datennutzungszweck: list[str] = Field([], alias="Datennutzungszweck")
    herausgeber: str | None = Field(None, alias="Herausgeber")
    kommentar: str | None = Field(None, alias="Kommentar")
    identifier: MergedIdentifier = Field(..., alias="MEx-Identifier")
    entityType: Annotated[
        Literal["DatenkompassResource"],
        Field(alias="$type", frozen=True, exclude=True),  # exclude when exporting
    ] = "DatenkompassResource"


AnyDatenkompassModel = (
    DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource
)

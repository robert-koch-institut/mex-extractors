from mex.common.models import (
    MergedActivity,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPrimarySource,
)
from mex.common.types import (
    Link,
    MergedActivityIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
    Text,
)
from mex.extractors.datenkompass.item import DatenkompassActivity


def mocked_merged_activities() -> list[MergedActivity]:
    return [
        MergedActivity(
            contact=["LoremIpsum1234"],
            responsibleUnit=[
                MergedOrganizationalUnitIdentifier("IdentifierOrgUnitZB"),
                MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG"),
            ],
            title=[
                Text(value="title no language"),
                Text(value="titel en", language="en"),
            ],
            abstract=[
                Text(value="Die Nutzung", language="de"),
                Text(value="The usage", language="en"),
            ],
            funderOrCommissioner=[
                MergedOrganizationIdentifier("Identifier2forBMG"),
                MergedOrganizationIdentifier("NoBMGIdentifier"),
            ],
            shortName=[
                Text(value="short en", language="en"),
                Text(value="short de", language="de"),
            ],
            theme=["https://mex.rki.de/item/theme-1"],  # PUBLIC_HEALTH
            website=[
                Link(language=None, title="Eintrag", url="https://www.Eintrag.de"),
                Link(url="https://www.weiterer_Eintrag.org"),
            ],
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityWithBMG2"),
        ),
        MergedActivity(
            contact=["LoremIpsum3456"],
            responsibleUnit=[MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG")],
            title=[
                Text(value="titel de", language="de"),
                Text(value="title en", language="en"),
            ],
            abstract=[Text(value="Without language", language=None)],
            funderOrCommissioner=[MergedOrganizationIdentifier("Identifier1forBMG")],
            shortName=[Text(value="short ony english", language="en")],
            theme=["https://mex.rki.de/item/theme-1"],  # PUBLIC_HEALTH
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityWithBMG1"),
        ),
        MergedActivity(
            contact=["LoremIpsum5678"],
            responsibleUnit=["DolorSitAmetConsec"],
            title=[Text(value="should not get extracted", language="de")],
            funderOrCommissioner=[MergedOrganizationIdentifier("NoBMGIdentifier")],
            theme=["https://mex.rki.de/item/theme-1"],  # PUBLIC_HEALTH
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityNoBMG"),
        ),
    ]


def mocked_merged_organizational_units() -> list[MergedOrganizationalUnit]:
    return [
        MergedOrganizationalUnit(
            name=[Text(value="example unit", language="en")],
            parentUnit=MergedOrganizationalUnitIdentifier("identifierParentUnit"),
            email=["unit@example.org"],
            shortName=[Text(value="e.g. unit", language="en")],
            entityType="MergedOrganizationalUnit",
            identifier=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG"),
        ),
        MergedOrganizationalUnit(
            name=[Text(value="andere Beispiel unit", language="de")],
            parentUnit=MergedOrganizationalUnitIdentifier("identifierParentUnit"),
            email=[],
            shortName=[Text(value="a.bsp. unit", language="en")],
            entityType="MergedOrganizationalUnit",
            identifier=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitZB"),
        ),
        MergedOrganizationalUnit(
            name=[Text(value="non-extractable unit", language="en")],
            parentUnit=MergedOrganizationalUnitIdentifier("identifierParentUnit"),
            email=["dont_extract@example.org"],
            shortName=[Text(value="nope", language="en")],
            entityType="MergedOrganizationalUnit",
            identifier=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitNo"),
        ),
    ]


def mocked_bmg() -> list[MergedOrganization]:
    return [
        MergedOrganization(
            officialName=[
                Text(value="Bundesministerium für Gesundheit", language="de")
            ],
            entityType="MergedOrganization",
            identifier=MergedOrganizationIdentifier("Identifier2forBMG"),
        ),
        MergedOrganization(
            officialName=[Text(value="BMG", language=None)],
            entityType="MergedOrganization",
            identifier=MergedOrganizationIdentifier("Identifier1forBMG"),
        ),
    ]


def mocked_merged_primary_sources() -> list[MergedPrimarySource]:
    return [
        MergedPrimarySource(
            entityType="MergedPrimarySource",
            identifier=MergedPrimarySourceIdentifier("00000000000000"),
        ),
        MergedPrimarySource(
            title=[Text(value="Awesome Primary Source", language="en")],
            entityType="MergedPrimarySource",
            identifier=MergedPrimarySourceIdentifier("identifierAwesomePS"),
        ),
    ]


def mocked_datenkompass_activity() -> list[DatenkompassActivity]:
    return [
        DatenkompassActivity(
            beschreibung="Die Nutzung",
            datenhalter="BMG",
            kontakt=[
                "a.bsp. unit",
                "e.g. unit",
                "unit@example.org",
            ],
            titel=["short de", "title no language"],
            schlagwort=["Public Health"],
            datenbank=[
                "https://www.Eintrag.de",
                "https://www.weiterer_Eintrag.org",
            ],
            voraussetzungen="Unbekannt",
            hauptkategorie="Gesundheit",
            unterkategorie="Public Health",
            rechtsgrundlage="Nicht bekannt",
            datenerhalt="Externe Zulieferung",
            status="Unbekannt",
            datennutzungszweck="Themenspezifische Auswertung",
            herausgeber="Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein.)"
            ),
            format="Projekt/Vorhaben",
            identifier="MergedActivityWithBMG2",
            entityType="MergedActivity",
        ),
        DatenkompassActivity(
            beschreibung="Without language",
            datenhalter="BMG",
            kontakt=[
                "e.g. unit",
                "unit@example.org",
            ],
            titel=["short ony english", "titel de"],
            schlagwort=["Public Health"],
            datenbank=[],
            voraussetzungen="Unbekannt",
            hauptkategorie="Gesundheit",
            unterkategorie="Public Health",
            rechtsgrundlage="Nicht bekannt",
            datenerhalt="Externe Zulieferung",
            status="Unbekannt",
            datennutzungszweck="Themenspezifische Auswertung",
            herausgeber="Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein.)"
            ),
            format="Projekt/Vorhaben",
            identifier="MergedActivityWithBMG1",
            entityType="MergedActivity",
        ),
    ]

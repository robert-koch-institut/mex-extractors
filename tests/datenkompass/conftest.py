import pytest

from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
)
from mex.common.models.primary_source import PreviewPrimarySource
from mex.common.types import (
    AccessRestriction,
    Link,
    MergedActivityIdentifier,
    MergedBibliographicResourceIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    Text,
)
from mex.extractors.datenkompass.models.item import DatenkompassActivity


@pytest.fixture
def mocked_merged_activities() -> list[MergedActivity]:
    """Mock a list of Merged Activity items."""
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
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_...
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
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ...
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityWithBMG1"),
        ),
        MergedActivity(
            contact=["LoremIpsum5678"],
            responsibleUnit=["DolorSitAmetConsec"],
            title=[Text(value="should get filtered out", language="en")],
            funderOrCommissioner=[MergedOrganizationIdentifier("NoBMGIdentifier")],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ..
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityNoBMG"),
        ),
    ]


@pytest.fixture
def mocked_merged_bibliographic_resource() -> list[MergedBibliographicResource]:
    """Mock a list of Merged Bibliographic Resource items."""
    return [
        MergedBibliographicResource(
            accessRestriction=AccessRestriction["OPEN"],
            title=[
                Text(value="title no language"),
                Text(value="titel en", language="en"),
            ],
            abstract=[
                Text(value="Die Nutzung", language="de"),
                Text(value="The usage", language="en"),
            ],
            contributingUnit=[
                MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG")
            ],
            keyword=[
                Text(value="short en", language="en"),
                Text(value="short de", language="de"),
            ],
            doi="https://doi.org/10.1234_find_this_first",
            alternateIdentifier=["find_second_a", "find_second_b"],
            repositoryURL=["https://www.find_third.to"],
            bibliographicResourceType=[
                "https://mex.rki.de/item/bibliographic-resource-type-1"
            ],  # BOOK
            creator=["PersonIdentifier4Peppa"],
            entityType="MergedBibliographicResource",
            identifier=MergedBibliographicResourceIdentifier("MergedBibResource1"),
        ),
    ]


@pytest.fixture
def mocked_merged_organizational_units() -> list[MergedOrganizationalUnit]:
    """Mock a list of Merged Organizational Unit items."""
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


@pytest.fixture
def mocked_bmg() -> list[MergedOrganization]:
    """Mock a list of BMG as Merged Organization items."""
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


@pytest.fixture
def mocked_merged_person() -> list[MergedPerson]:
    """Mock a single Merged Person item."""
    return [
        MergedPerson(
            fullName=["Pattern, Peppa P.", "Pattern, P.P."],
            entityType="MergedPerson",
            identifier=MergedPersonIdentifier("PersonIdentifier4Peppa"),
        )
    ]


@pytest.fixture
def mocked_preview_primary_sources() -> list[PreviewPrimarySource]:
    """Mock a list of Preview Primary Source items."""
    return [
        PreviewPrimarySource(
            entityType="PreviewPrimarySource",
            identifier="SomeIrrelevantPS",
        ),
        PreviewPrimarySource(
            title=[Text(value="this is a Relevant Primary Source", language="en")],
            entityType="PreviewPrimarySource",
            identifier="identifierRelevantPS",
        ),
    ]


@pytest.fixture
def mocked_datenkompass_activity() -> list[DatenkompassActivity]:
    """Mock a list of Datenkompass Activity items."""
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
            schlagwort=["Infektionskrankheiten und -epidemiologie"],
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
                "voraussichtlich Ende 2025 verfügbar sein."
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
            schlagwort=["Infektionskrankheiten und -epidemiologie"],
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
                "voraussichtlich Ende 2025 verfügbar sein."
            ),
            format="Projekt/Vorhaben",
            identifier="MergedActivityWithBMG1",
            entityType="MergedActivity",
        ),
    ]

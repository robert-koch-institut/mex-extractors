from itertools import groupby

import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    Identifier,
    Link,
    MergedOrganizationIdentifier,
    MergedResourceIdentifier,
    TemporalEntity,
    Text,
    TextLanguage,
)
from mex.extractors.settings import Settings
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.synopse.transform import (
    transform_synopse_variables_to_mex_variable_groups,
)
from mex.extractors.utils import load_yaml


@pytest.fixture
def extracted_person() -> ExtractedPerson:
    """Return an extracted person with static dummy values."""
    return ExtractedPerson(
        affiliation=Identifier.generate(23),
        email=["fictitiousf@rki.de", "info@rki.de"],
        familyName="Fictitious",
        fullName="Fictitious, Frieda, Dr.",
        givenName="Frieda",
        memberOf=Identifier.generate(256),
        hadPrimarySource=Identifier.generate(40),
        identifierInPrimarySource="frieda",
    )


@pytest.fixture
def extracted_organization() -> list[ExtractedOrganization]:
    """Return an extracted person with static dummy values."""
    return [
        ExtractedOrganization(
            hadPrimarySource=Identifier.generate(40),
            identifierInPrimarySource="org",
            officialName="org name",
        )
    ]


@pytest.fixture
def synopse_variables_raw() -> list[dict[str, str | int | float | None]]:
    """Return a list of dicts in the required format for Synopse Variables."""
    return [
        {  # var 1, auspraegung 1
            "textbox49": -97,
            "Originalfrage": None,
            "StudieID1": "STUDY1",
            "StudieID2": 12345,
            "SymopseID": 1,
            "textbox51": "Nicht erhoben",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "valInstrument": "Health Questionnaire , Frage 18",
            "textbox21": "Angeborene Fehlbildung",
            "textbox24": "KHEfehlb",
            "textbox11": "Zahl",
            "IntVar": False,
            "KeepVarname": False,
        },
        {  # var 1, auspraegung 2
            "textbox49": -98,
            "Originalfrage": None,
            "StudieID1": "STUDY1",
            "StudieID2": 12345,
            "SymopseID": 1,
            "textbox51": "Weiß nicht",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "valInstrument": None,
            "textbox21": "Angeborene Fehlbildung",
            "textbox24": "KHEfehlb",
            "textbox11": "Text",
            "IntVar": True,
            "KeepVarname": False,
        },
        {  # var 2, missing var label, valInstrument
            "textbox49": 1,
            "Originalfrage": None,
            "StudieID1": "STUDY1",
            "StudieID2": 12345,
            "SymopseID": 2,
            "textbox51": "Ja",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "valInstrument": None,
            "textbox21": None,
            "textbox24": "KHEfiebB",
            "textbox11": "Zahl",
            "IntVar": True,
            "KeepVarname": False,
        },
        {  # var 3, no auspraegung
            "textbox49": None,
            "Originalfrage": None,
            "StudieID1": "STUDY1",
            "StudieID2": 12345,
            "SymopseID": 3,
            "textbox51": None,
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "valInstrument": None,
            "textbox21": "no auspraegung",
            "textbox24": "no_auspraegung",
            "textbox11": "Text",
            "IntVar": False,
            "KeepVarname": False,
        },
        {  # var 4, different value in textbox5
            "textbox49": None,
            "Originalfrage": None,
            "StudieID1": "STUDY1",
            "StudieID2": 12345,
            "SymopseID": 4,
            "textbox51": None,
            "textbox5": "Gesundheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "valInstrument": None,
            "textbox21": "no auspraegung",
            "textbox24": "no_auspraegung",
            "textbox11": "Zahl",
            "IntVar": False,
            "KeepVarname": False,
        },
        {  # var 5, different studie_id, same thema
            "textbox49": None,
            "Originalfrage": None,
            "StudieID1": "STUDY2",
            "StudieID2": 23456,
            "SymopseID": 5,
            "textbox51": None,
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "valInstrument": None,
            "textbox21": "no auspraegung",
            "textbox24": "no_auspraegung",
            "textbox11": "Text",
            "IntVar": False,
            "KeepVarname": False,
        },
    ]


@pytest.fixture
def synopse_variables(
    synopse_variables_raw: list[dict[str, str | int]],
) -> list[SynopseVariable]:
    """Return a list Synopse Variables."""
    return [SynopseVariable.model_validate(v) for v in synopse_variables_raw]


@pytest.fixture
def synopse_variables_by_thema(
    synopse_variables: list[SynopseVariable],
) -> dict[str, list[SynopseVariable]]:
    """Return a mapping from synopse thema to the variables with this thema."""
    synopse_variables = sorted(
        synopse_variables, key=lambda v: v.thema_und_fragebogenausschnitt
    )
    return {
        thema: list(variables)
        for thema, variables in groupby(
            synopse_variables, key=lambda v: v.thema_und_fragebogenausschnitt
        )
    }


@pytest.fixture
def synopse_studies() -> list[SynopseStudy]:
    """Return a list of Synopse Studies."""
    return [
        SynopseStudy(
            beschreibung="ein heikles Unterfangen.",
            dokumentation="Z:\\foo\\bar",
            ds_typ_id=17,
            erstellungs_datum="2022",
            feld_ende="2013",
            feld_start="2000",
            plattform_adresse="S:\\data",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="12345",
            titel_datenset="Titel",
            Studie="Studie123",
            Zugangsbeschraenkung="open",
        ),
        SynopseStudy(
            beschreibung="ein zweites heikles Unterfangen.",
            dokumentation="X:\\foo\\bar",
            ds_typ_id=16,
            erstellungs_datum="2017",
            plattform_adresse="blabli blubb",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="123456",
            titel_datenset="Titel 2",
            Zugangsbeschraenkung="open",
        ),
        SynopseStudy(
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            dokumentation="interne Datennutzung",
            ds_typ_id=16,
            erstellungs_datum="2017",
            plattform_adresse="blabli blubb",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="123457",
            titel_datenset="Study ohne Referenzen",
            Zugangsbeschraenkung="open",
        ),
        SynopseStudy(
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            dokumentation="https://asd.def",
            ds_typ_id=16,
            erstellungs_datum="2017",
            plattform_adresse="blabli blubb",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="123458",
            titel_datenset="Study 2 ohne Referenzen",
            Zugangsbeschraenkung="open",
        ),
        SynopseStudy(
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            dokumentation="interne Datennutzung",
            ds_typ_id=16,
            erstellungs_datum="2017",
            plattform_adresse="interne Datennutzung",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="123457",
            titel_datenset="Study ohne Referenzen",
            Zugangsbeschraenkung="open",
        ),
        SynopseStudy(
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            dokumentation="https://asd.def",
            ds_typ_id=16,
            erstellungs_datum="2017",
            plattform_adresse="https://asd.def",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="123458",
            titel_datenset="Study 2 ohne Referenzen",
            Zugangsbeschraenkung="open",
        ),
    ]


@pytest.fixture
def synopse_organization_ids_by_query_string() -> dict[
    str, MergedOrganizationIdentifier
]:
    """Return merged organizations id by org name."""
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    return {"Test-Institute": organization_id}


@pytest.fixture
def synopse_projects() -> list[SynopseProject]:
    """Return a list of Synopse Projects."""
    return [
        SynopseProject(
            akronym_des_studientitels="BBCCDD_00",
            anschlussprojekt="BBCCDD",
            beitragende="Carla Contact",
            beschreibung_der_studie="BBCCDD-Basiserhebung am RKI.",
            externe_partner="Testpartner",
            project_studientitel="Studie zu Lorem und Ipsum",
            kontakt=["info@rki.de"],
            projektbeginn="1999",
            projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            projektende="2000",
            studien_id="12345",
            studienart_studientyp="Monitoring-Studie",
            verantwortliche_oe="C1",
        ),
        SynopseProject(
            akronym_des_studientitels="BBCCDD",
            beitragende="Carla Contact",
            beschreibung_der_studie="BBCCDD-Basiserhebung am RKI.",
            project_studientitel="Studie zu Lorem und Ipsum",
            kontakt=["info@rki.de"],
            projektbeginn="1999",
            projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            projektende="2000",
            studien_id="12346",
            studienart_studientyp="Monitoring-Studie",
            verantwortliche_oe="C1",
        ),
    ]


@pytest.fixture
def synopse_project(synopse_projects: list[SynopseProject]) -> SynopseProject:
    """Return a Synopse Project."""
    return synopse_projects[0]


@pytest.fixture
def extracted_access_platforms(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> list[ExtractedAccessPlatform]:
    """Return a list of extracted access platforms."""
    return [
        ExtractedAccessPlatform(
            contact=[Identifier.generate(seed=234)],
            hadPrimarySource=extracted_primary_sources["report-server"].stableTargetId,
            identifierInPrimarySource="S:\\data",
            landingPage=[Link(url="file:///Z:/data")],
            technicalAccessibility="https://mex.rki.de/item/technical-accessibility-1",
            title=[Text(value="Z:\\data")],
            unitInCharge=[Identifier.generate(seed=234)],
        ),
        ExtractedAccessPlatform(
            contact=[Identifier.generate(seed=234)],
            hadPrimarySource=extracted_primary_sources["report-server"].stableTargetId,
            identifierInPrimarySource="blabli blubb",
            landingPage=[Link(url="blabli blubb")],
            technicalAccessibility="https://mex.rki.de/item/technical-accessibility-1",
            title=[Text(value="blabli blubb")],
            unitInCharge=[Identifier.generate(seed=234)],
        ),
    ]


@pytest.fixture
def extracted_activity(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> ExtractedActivity:
    """Return an extracted activity."""
    return ExtractedActivity(
        abstract=[{"value": "Die Studie untersucht die Laune."}],
        activityType=["https://mex.rki.de/item/activity-type-6"],
        contact=[Identifier.generate(seed=123)],
        documentation=[
            Link(
                url="file:///Z:/Projekte/Dokumentation",
                title="- Fragebogen\n- Labor",
            )
        ],
        end=[TemporalEntity("2013")],
        hadPrimarySource=extracted_primary_sources["report-server"].stableTargetId,
        identifierInPrimarySource="12345",
        involvedPerson=[Identifier.generate(seed=12)],
        responsibleUnit=[Identifier.generate(seed=13)],
        shortName=[{"value": "BBCCDD_00"}],
        start=[TemporalEntity("2000")],
        theme=["https://mex.rki.de/item/theme-36"],
        title=[Text(language=TextLanguage.DE, value="Studie zu Lorem und Ipsum")],
    )


@pytest.fixture
def resource_ids_by_synopse_id() -> dict[str, list[MergedResourceIdentifier]]:
    """Return a lookup from study ID to list of resource IDs."""
    return {
        "1": [MergedResourceIdentifier.generate(seed=42)],
        "2": [MergedResourceIdentifier.generate(seed=43)],
        "3": [MergedResourceIdentifier.generate(seed=42)],
        "4": [MergedResourceIdentifier.generate(seed=42)],
        "5": [MergedResourceIdentifier.generate(seed=45)],
    }


@pytest.fixture
def extracted_variable_groups(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    resource_ids_by_synopse_id: dict[str, list[MergedResourceIdentifier]],
) -> list[ExtractedVariableGroup]:
    """Return a list of extracted variable groups."""
    return list(
        transform_synopse_variables_to_mex_variable_groups(
            synopse_variables_by_thema,
            extracted_primary_sources["report-server"],
            resource_ids_by_synopse_id,
        )
    )


@pytest.fixture
def synopse_access_platform(settings: Settings) -> AccessPlatformMapping:
    """Return a mapping model with access platform default values."""
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "access-platform_mock.yaml")
    )


@pytest.fixture
def synopse_activity(settings: Settings) -> ActivityMapping:
    """Return a mapping model with activity default values."""
    return ActivityMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "activity_mock.yaml")
    )


@pytest.fixture
def synopse_resource(settings: Settings) -> ResourceMapping:
    """Return a mapping model with resource default values."""
    return ResourceMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "resource_mock.yaml")
    )

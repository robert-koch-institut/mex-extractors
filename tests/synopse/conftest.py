from itertools import groupby

import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedActivity,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    AccessRestriction,
    Identifier,
    Link,
    MergedOrganizationIdentifier,
    TemporalEntity,
    Text,
    TextLanguage,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.synopse.models.project import ProjektUndStudienverwaltung
from mex.extractors.synopse.models.study import MetadatenZuDatensaetzen
from mex.extractors.synopse.models.study_overview import Datensatzuebersicht
from mex.extractors.synopse.models.variable import Variablenuebersicht
from mex.extractors.synopse.transform import (
    transform_synopse_variables_to_mex_variable_groups,
)
from mex.extractors.utils import load_yaml


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
    ]


@pytest.fixture
def synopse_study_overviews() -> list[Datensatzuebersicht]:
    """Return a list Synopse Study Overviews."""
    return [
        Datensatzuebersicht(
            StudienID="12345",
            DStypID=17,
            Titel_Datenset="set1",
            SynopseID="synopse1",
        ),
        Datensatzuebersicht(
            StudienID="studie1",
            DStypID=18,
            Titel_Datenset="set2",
            SynopseID="synopse1",
        ),
        Datensatzuebersicht(
            StudienID="studie2",
            DStypID=32,
            Titel_Datenset="set2",
            SynopseID="synopse2",
        ),
    ]


@pytest.fixture
def synopse_resources() -> list[ExtractedResource]:
    """Return a list of synopse resources."""
    return [
        ExtractedResource(
            title="Found in overview",
            identifierInPrimarySource="12345-set1-17",
            hadPrimarySource=Identifier.generate(),
            accessRestriction=AccessRestriction["OPEN"],
            contact=[Identifier.generate()],
            unitInCharge=[Identifier.generate()],
            theme="https://mex.rki.de/item/theme-36",
        ),
        ExtractedResource(
            title="Found in overview too",
            identifierInPrimarySource="studie1-set2-18",
            hadPrimarySource=Identifier.generate(),
            accessRestriction=AccessRestriction["OPEN"],
            contact=[Identifier.generate()],
            unitInCharge=[Identifier.generate()],
            theme="https://mex.rki.de/item/theme-36",
        ),
        ExtractedResource(
            title="Not found in overview",
            identifierInPrimarySource="not-found",
            hadPrimarySource=Identifier.generate(),
            accessRestriction=AccessRestriction["OPEN"],
            contact=[Identifier.generate()],
            unitInCharge=[Identifier.generate()],
            theme="https://mex.rki.de/item/theme-36",
        ),
    ]


@pytest.fixture
def synopse_variables(
    synopse_variables_raw: list[dict[str, str | int]],
) -> list[Variablenuebersicht]:
    """Return a list Synopse Variables."""
    return [Variablenuebersicht.model_validate(v) for v in synopse_variables_raw]


@pytest.fixture
def synopse_variables_by_study_id(
    synopse_variables: list[Variablenuebersicht],
) -> dict[int, list[Variablenuebersicht]]:
    """Return a mapping from synopse studie id to the variables with this studie id."""
    synopse_variables = sorted(synopse_variables, key=lambda v: v.StudieID2)
    return {
        StudieID2: list(variables)
        for StudieID2, variables in groupby(
            synopse_variables, key=lambda v: v.StudieID2
        )
    }


@pytest.fixture
def synopse_variables_by_thema(
    synopse_variables: list[Variablenuebersicht],
) -> dict[str, list[Variablenuebersicht]]:
    """Return a mapping from synopse thema to the variables with this thema."""
    synopse_variables = sorted(synopse_variables, key=lambda v: v.textbox5)
    return {
        thema: list(variables)
        for thema, variables in groupby(synopse_variables, key=lambda v: v.textbox5)
    }


@pytest.fixture
def synopse_studies() -> list[MetadatenZuDatensaetzen]:
    """Return a list of Synopse Studies."""
    return [
        MetadatenZuDatensaetzen(
            Beitragende="Jane Doe",
            Beschreibung="ein heikles Unterfangen.",
            # dokumentation="Z:\\foo\\bar",
            DStypID=17,
            # erstellungs_datum="2022",
            plattform_adresse="S:\\data",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            StudienID="12345",
            Titel_Datenset="Titel",
            Studie="Studie123",
            zugangsbeschraenkung="protected",
        ),
        MetadatenZuDatensaetzen(
            Beschreibung="ein zweites heikles Unterfangen.",
            # dokumentation="X:\\foo\\bar",
            DStypID=16,
            # erstellungs_datum="2017",
            plattform_adresse="blabli blubb",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            StudienID="123456",
            Titel_Datenset="Titel 2",
            zugangsbeschraenkung="open",
        ),
        MetadatenZuDatensaetzen(
            Beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            # dokumentation="interne Datennutzung",
            DStypID=16,
            # erstellungs_datum="2017",
            plattform_adresse="blabli blubb",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            StudienID="123457",
            Titel_Datenset="Study ohne Referenzen",
            zugangsbeschraenkung="sensitive",
        ),
        MetadatenZuDatensaetzen(
            Beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            # dokumentation="https://asd.def",
            DStypID=16,
            erstellungs_datum="2017",
            plattform_adresse="blabli blubb",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            StudienID="123458",
            Titel_Datenset="Study 2 ohne Referenzen",
            zugangsbeschraenkung="protected",
        ),
        MetadatenZuDatensaetzen(
            Beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            # dokumentation="interne Datennutzung",
            DStypID=16,
            ##erstellungs_datum="2017",
            plattform_adresse="interne Datennutzung",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            StudienID="123457",
            Titel_Datenset="Study ohne Referenzen",
            zugangsbeschraenkung="sensitive",
        ),
        MetadatenZuDatensaetzen(
            Beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            # dokumentation="https://asd.def",
            DStypID=16,
            erstellungs_datum="2017",
            plattform_adresse="https://asd.def",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            StudienID="123458",
            Titel_Datenset="Study 2 ohne Referenzen",
            zugangsbeschraenkung="open",
        ),
    ]


@pytest.fixture
def synopse_access_platform() -> AccessPlatformMapping:
    """Return a list of extracted access platforms."""
    settings = Settings.get()
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "access-platform.yaml")
    )


@pytest.fixture
def created_by_study_id(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> dict[str, str]:
    """Return a lookup from study ID to created string."""
    return {
        s.StudienID: s.erstellungs_datum for s in synopse_studies if s.erstellungs_datum
    }


@pytest.fixture
def description_by_study_id(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> dict[str, str]:
    """Return a lookup from study ID to description string."""
    return {s.StudienID: s.Beschreibung for s in synopse_studies if s.Beschreibung}


@pytest.fixture
def documentation_by_study_id(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> dict[str, Link]:
    """Return a lookup from study ID to documentation Link."""
    return {
        s.StudienID: Link(url=s.dokumentation)
        for s in synopse_studies
        if s.dokumentation
    }


@pytest.fixture
def keyword_text_by_study_id(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> dict[str, list[Text]]:
    """Return a lookup from study ID to list of keyword Text."""
    return {
        s.StudienID: [Text(value=s.schlagworte_themen)]
        for s in synopse_studies
        if s.schlagworte_themen
    }


@pytest.fixture
def synopse_study(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> MetadatenZuDatensaetzen:
    """Return a Synopse Study."""
    return synopse_studies[0]


@pytest.fixture
def synopse_merged_organization_ids_by_query_string() -> dict[
    str, MergedOrganizationIdentifier
]:
    """Return merged organizations id by org name."""
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    return {"Test-Institute": organization_id}


@pytest.fixture
def synopse_projects() -> list[ProjektUndStudienverwaltung]:
    """Return a list of Synopse Projects."""
    return [
        ProjektUndStudienverwaltung(
            Studie="BBCCDD_00",
            Anschlussprojekt="BBCCDD",
            Beitragende="Roland Resolved",
            BeschreibungStudie="BBCCDD-Basiserhebung am RKI.",
            Partner_extern="Testpartner",
            ProjektStudientitel="Studie zu Lorem und Ipsum",
            Kontakt=["info@rki.de"],
            Projektbeginn="1999",
            Projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            Projektende="2000",
            StudienID="12345",
            StudienArtTyp="Monitoring-Studie",
            VerantwortlicheOE="C1",
            Partner_intern="fg99, C1",
        ),
        ProjektUndStudienverwaltung(
            Studie="BBCCDD",
            Beitragende="Roland Resolved",
            BeschreibungStudie="BBCCDD-Basiserhebung am RKI.",
            ProjektStudientitel="Studie zu Lorem und Ipsum",
            Kontakt=["info@rki.de"],
            Projektbeginn="1999",
            Projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            Projektende="2000",
            StudienID="12346",
            StudienArtTyp="Monitoring-Studie",
            VerantwortlicheOE="C1",
        ),
    ]


@pytest.fixture
def synopse_project(
    synopse_projects: list[ProjektUndStudienverwaltung],
) -> ProjektUndStudienverwaltung:
    """Return a Synopse Project."""
    return synopse_projects[0]


@pytest.fixture
def extracted_activity() -> ExtractedActivity:
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
        hadPrimarySource=get_extracted_primary_source_id_by_name("report-server"),
        identifierInPrimarySource="12345",
        involvedPerson=[Identifier.generate(seed=12)],
        responsibleUnit=[Identifier.generate(seed=13)],
        shortName=[{"value": "BBCCDD_00"}],
        start=[TemporalEntity("2000")],
        theme=["https://mex.rki.de/item/theme-36"],
        title=[Text(language=TextLanguage.DE, value="Studie zu Lorem und Ipsum")],
    )


@pytest.fixture
def resources_by_synopse_id(
    synopse_resources: list[ExtractedResource],
) -> dict[str, ExtractedResource]:
    """Return a lookup from study ID to resources."""
    return {
        "12345-set1-17": synopse_resources[0],
        "2": synopse_resources[1],
        "4": synopse_resources[2],
    }


@pytest.fixture
def extracted_variable_groups(
    synopse_variables_by_thema: dict[str, list[Variablenuebersicht]],
    resources_by_synopse_id: dict[str, ExtractedResource],
    synopse_study_overviews: list[Datensatzuebersicht],
) -> list[ExtractedVariableGroup]:
    """Return a list of extracted variable groups."""
    return transform_synopse_variables_to_mex_variable_groups(
        synopse_variables_by_thema,
        resources_by_synopse_id,
        synopse_study_overviews,
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

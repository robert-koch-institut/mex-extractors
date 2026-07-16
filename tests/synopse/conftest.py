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
            "originalfrage": None,
            "studie_id2": 12345,
            "symopse_id": 1,
            "textbox51": "Nicht erhoben",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "val_instrument": "Health Questionnaire , Frage 18",
            "textbox21": "Angeborene Fehlbildung",
            "textbox24": "KHEfehlb",
            "textbox11": "Zahl",
            "int_var": False,
            "KeepVarname": False,
        },
        {  # var 1, auspraegung 2
            "textbox49": -98,
            "originalfrage": None,
            "studie_id2": 12345,
            "symopse_id": 1,
            "textbox51": "Weiß nicht",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "val_instrument": None,
            "textbox21": "Angeborene Fehlbildung",
            "textbox24": "KHEfehlb",
            "textbox11": "Text",
            "int_var": True,
            "KeepVarname": False,
        },
        {  # var 2, missing var label, val_instrument
            "textbox49": 1,
            "originalfrage": None,
            "studie_id2": 12345,
            "symopse_id": 2,
            "textbox51": "Ja",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "val_instrument": None,
            "textbox21": None,
            "textbox24": "KHEfiebB",
            "textbox11": "Zahl",
            "int_var": True,
            "KeepVarname": False,
        },
    ]


@pytest.fixture
def synopse_study_overviews() -> list[Datensatzuebersicht]:
    """Return a list Synopse Study Overviews."""
    return [
        Datensatzuebersicht(
            studien_id="12345",
            dstyp_id=17,
            titel_datenset="set1",
            synopse_id="synopse1",
        ),
        Datensatzuebersicht(
            studien_id="studie1",
            dstyp_id=18,
            titel_datenset="set2",
            synopse_id="synopse1",
        ),
        Datensatzuebersicht(
            studien_id="studie2",
            dstyp_id=32,
            titel_datenset="set2",
            synopse_id="synopse2",
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
    synopse_variables = sorted(synopse_variables, key=lambda v: v.studie_id2)
    return {
        studie_id2: list(variables)
        for studie_id2, variables in groupby(
            synopse_variables, key=lambda v: v.studie_id2
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
            studien_id="12345",
            dstyp_id=17,
            titel_datenset="Titel",
            beschreibung="ein heikles Unterfangen.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            rechte="Niemand darf irgendwas.",
            zugangsbeschraenkung="protected",
            datum_der_letzten_aenderung="2022",
        ),
        MetadatenZuDatensaetzen(
            studien_id="123456",
            dstyp_id=16,
            titel_datenset="Titel 2",
            beschreibung="ein zweites heikles Unterfangen.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            rechte="Niemand darf irgendwas.",
            zugangsbeschraenkung="open",
            datum_der_letzten_aenderung="2017",
        ),
        MetadatenZuDatensaetzen(
            studien_id="123457",
            dstyp_id=16,
            titel_datenset="Study ohne Referenzen",
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            rechte="Niemand darf irgendwas.",
            zugangsbeschraenkung="sensitive",
            datum_der_letzten_aenderung="2017",
        ),
        MetadatenZuDatensaetzen(
            studien_id="123458",
            dstyp_id=16,
            titel_datenset="Study 2 ohne Referenzen",
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            rechte="Niemand darf irgendwas.",
            zugangsbeschraenkung="protected",
            datum_der_letzten_aenderung="2017",
        ),
        MetadatenZuDatensaetzen(
            studien_id="123457",
            dstyp_id=16,
            titel_datenset="Study ohne Referenzen",
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            rechte="Niemand darf irgendwas.",
            zugangsbeschraenkung="sensitive",
            datum_der_letzten_aenderung="2018",
        ),
        MetadatenZuDatensaetzen(
            studien_id="123458",
            dstyp_id=16,
            titel_datenset="Study 2 ohne Referenzen",
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            rechte="Niemand darf irgendwas.",
            zugangsbeschraenkung="open",
            datum_der_letzten_aenderung="2017",
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
def description_by_study_id(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> dict[str, str]:
    """Return a lookup from study ID to description string."""
    return {s.studien_id: s.beschreibung for s in synopse_studies if s.beschreibung}


@pytest.fixture
def keyword_text_by_study_id(
    synopse_studies: list[MetadatenZuDatensaetzen],
) -> dict[str, list[Text]]:
    """Return a lookup from study ID to list of keyword Text."""
    return {
        s.studien_id: [Text(value=s.schlagworte_themen)]
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
            studie="BBCCDD_00",
            anschlussprojekt="BBCCDD",
            beitragende="Roland Resolved",
            BeschreibungStudie="BBCCDD-Basiserhebung am RKI.",
            partner_extern="Testpartner",
            projekt_studientitel="studie zu Lorem und Ipsum",
            Kontakt=["info@rki.de"],
            projektbeginn="1999",
            projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            projektende="2000",
            studien_id="12345",
            studien_art_typ="Monitoring-studie",
            verantwortliche_oe="C1",
            partner_intern="fg99, C1",
        ),
        ProjektUndStudienverwaltung(
            studie="BBCCDD",
            beitragende="Roland Resolved",
            BeschreibungStudie="BBCCDD-Basiserhebung am RKI.",
            projekt_studientitel="studie zu Lorem und Ipsum",
            Kontakt=["info@rki.de"],
            projektbeginn="1999",
            projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            projektende="2000",
            studien_id="12346",
            studien_art_typ="Monitoring-studie",
            verantwortliche_oe="C1",
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
        abstract=[{"value": "Die studie untersucht die Laune."}],
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
        title=[Text(language=TextLanguage.DE, value="studie zu Lorem und Ipsum")],
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

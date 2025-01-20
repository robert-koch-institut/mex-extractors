from itertools import groupby

import pytest

from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.types import (
    AccessRestriction,
    Identifier,
    Language,
    Link,
    MergedOrganizationIdentifier,
    MergedResourceIdentifier,
    PersonalData,
    ResourceCreationMethod,
    ResourceTypeGeneral,
    TechnicalAccessibility,
    TemporalEntity,
    Text,
    TextLanguage,
    Theme,
)
from mex.extractors.mapping.transform import transform_mapping_data_to_model
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.synopse.transform import (
    transform_overviews_to_resource_lookup,
    transform_synopse_variables_to_mex_variable_groups,
)


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
            "IntVar": False,
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
            "IntVar": False,
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
def synopse_variables_by_study_id(
    synopse_variables: list[SynopseVariable],
) -> dict[int, list[SynopseVariable]]:
    """Return a mapping from synopse studie id to the variables with this studie id."""
    synopse_variables = sorted(synopse_variables, key=lambda v: v.studie_id)
    return {
        studie_id: list(variables)
        for studie_id, variables in groupby(
            synopse_variables, key=lambda v: v.studie_id
        )
    }


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
            plattform_adresse="Z:\\data",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="12345",
            titel_datenset="Titel",
            Studie="Studie123",
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
        ),
        SynopseStudy(
            beschreibung="eine study ohne Variablen, Projekt, oder exctractedActivity.",
            dokumentation="https://asd.def",
            ds_typ_id=16,
            erstellungs_datum="2017",
            plattform_adresse="noch nicht erstellt",
            rechte="Niemand darf irgendwas.",
            schlagworte_themen="Alkohol, Alter und Geschlecht, Drogen",
            studien_id="123458",
            titel_datenset="Study 2 ohne Referenzen",
        ),
    ]


@pytest.fixture
def created_by_study_id(synopse_studies) -> dict[str, str]:
    """Return a lookup from study ID to created string."""
    return {s.studien_id: s.erstellungs_datum for s in synopse_studies}


@pytest.fixture
def description_by_study_id(synopse_studies) -> dict[str, str]:
    """Return a lookup from study ID to description string."""
    return {s.studien_id: s.beschreibung for s in synopse_studies}


@pytest.fixture
def documentation_by_study_id(synopse_studies) -> dict[str, Link]:
    """Return a lookup from study ID to documentation Link."""
    return {s.studien_id: s.dokumentation for s in synopse_studies}


@pytest.fixture
def keyword_text_by_study_id(synopse_studies) -> dict[str, list[Text]]:
    """Return a lookup from study ID to list of keyword Text."""
    return {s.studien_id: s.schlagworte_themen for s in synopse_studies}


@pytest.fixture
def synopse_study(synopse_studies: list[SynopseStudy]) -> SynopseStudy:
    """Return a Synopse Study."""
    return synopse_studies[0]


@pytest.fixture
def synopse_organization_ids_by_query_string() -> (
    dict[str, MergedOrganizationIdentifier]
):
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
            project_studientitel="Studie zu Lorem und Ipsum",
            kontakt=["info@rki.de"],
            projektbeginn=1999,
            projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            projektende=2000,
            studien_id="12345",
            studienart_studientyp="Monitoring-Studie",
            verantwortliche_oe="FG 99",
        ),
        SynopseProject(
            akronym_des_studientitels="BBCCDD",
            beitragende="Carla Contact",
            beschreibung_der_studie="BBCCDD-Basiserhebung am RKI.",
            project_studientitel="Studie zu Lorem und Ipsum",
            kontakt=["info@rki.de"],
            projektbeginn=1999,
            projektdokumentation="""Z:\\Projekte\\Dokumentation

- Fragebogen
- Labor""",
            projektende=2000,
            studien_id="12346",
            studienart_studientyp="Monitoring-Studie",
            verantwortliche_oe="FG 99",
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
            identifierInPrimarySource="Z:\\data",
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
def extracted_resources() -> list[ExtractedResource]:
    """Return an list of extracted resources."""
    return [
        ExtractedResource(
            accessRestriction=AccessRestriction(
                "https://mex.rki.de/item/access-restriction-1"
            ),
            contact=Identifier.generate(seed=5),
            hadPrimarySource=Identifier.generate(seed=5),
            identifierInPrimarySource="23456-17-set1",
            theme="https://mex.rki.de/item/theme-11",
            title="Found in overview",
            unitInCharge=Identifier.generate(seed=6),
        ),
        ExtractedResource(
            accessRestriction=AccessRestriction(
                "https://mex.rki.de/item/access-restriction-1"
            ),
            contact=Identifier.generate(seed=5),
            hadPrimarySource=Identifier.generate(seed=5),
            identifierInPrimarySource="12345-12-set2",
            theme="https://mex.rki.de/item/theme-11",
            title="The other one",
            unitInCharge=Identifier.generate(seed=6),
        ),
        ExtractedResource(
            accessRestriction=AccessRestriction(
                "https://mex.rki.de/item/access-restriction-1"
            ),
            contact=Identifier.generate(seed=5),
            hadPrimarySource=Identifier.generate(seed=5),
            identifierInPrimarySource="12345-13-set13",
            theme="https://mex.rki.de/item/theme-11",
            title="The other one",
            unitInCharge=Identifier.generate(seed=6),
        ),
    ]


@pytest.fixture
def synopse_overviews() -> list[SynopseStudyOverview]:
    """Return list of Synopse Overviews."""
    return [
        SynopseStudyOverview(
            studien_id="23456",
            ds_typ_id=17,
            titel_datenset="set1",
            synopse_id="23456",
        ),
        SynopseStudyOverview(
            studien_id="23456",
            ds_typ_id=17,
            titel_datenset="set1",
            synopse_id="5",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=12,
            titel_datenset="set2",
            synopse_id="12345",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=12,
            titel_datenset="set2",
            synopse_id="1",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=13,
            titel_datenset="set13",
            synopse_id="1",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=12,
            titel_datenset="set2",
            synopse_id="2",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=12,
            titel_datenset="set2",
            synopse_id="3",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=None,
            titel_datenset="set2",
            synopse_id="12345678901111",
        ),
        SynopseStudyOverview(
            studien_id="12345",
            ds_typ_id=12,
            titel_datenset="set2",
            synopse_id="4",
        ),
    ]


@pytest.fixture
def resource_ids_by_synopse_id(
    extracted_resources: list[ExtractedResource],
    synopse_overviews: list[SynopseStudyOverview],
) -> dict[str, list[MergedResourceIdentifier]]:
    """Return a lookup from study ID to list of resource IDs."""
    return transform_overviews_to_resource_lookup(
        synopse_overviews, extracted_resources
    )


@pytest.fixture
def extracted_variable_groups(
    synopse_variables_by_thema: dict[int, list[SynopseVariable]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    resource_ids_by_synopse_id: dict[str, list[Identifier]],
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
def synopse_access_platform() -> AnyMappingModel:
    """Return a mapping model with access platform default values."""
    return transform_mapping_data_to_model(
        {
            "hadPrimarySource": [],
            "identifierInPrimarySource": [],
            "technicalAccessibility": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [TechnicalAccessibility["INTERNAL"]],
                            "rule": None,
                        }
                    ],
                    "comment": "internal",
                }
            ],
            "contact": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Use value",
                        }
                    ],
                    "comment": None,
                }
            ],
            "unitInCharge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Use value ",
                        }
                    ],
                    "comment": None,
                }
            ],
        },
        ExtractedAccessPlatform,
    )


@pytest.fixture
def synopse_activity() -> AnyMappingModel:
    """Return a mapping model with activity default values."""
    return transform_mapping_data_to_model(
        {
            "hadPrimarySource": [],
            "identifierInPrimarySource": [],
            "title": [],
            "responsibleUnit": [],
            "contact": [],
            "activityType": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/activity-type-6"]}
                    ],
                    "comment": "Sonstige",
                }
            ],
            "theme": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["7202001", "7202002", "7202003", "7202004"],
                            "setValues": ["https://mex.rki.de/item/theme-11"],
                        },
                        {"setValues": ["https://mex.rki.de/item/theme-36"]},
                    ],
                    "comment": "Studien und Surveillance",
                }
            ],
        },
        ExtractedActivity,
    )


@pytest.fixture
def synopse_resource() -> AnyMappingModel:
    """Return a mapping model with resource default values."""
    return transform_mapping_data_to_model(
        {
            "hadPrimarySource": [],
            "identifierInPrimarySource": [],
            "title": [],
            "accessRestriction": [
                {
                    "fieldInPrimarySource": "Zugangsbeschraenkung",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["restriktiv"],
                            "setValues": [AccessRestriction["RESTRICTED"]],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "hasPersonalData": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [PersonalData["PERSONAL_DATA"]],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "contact": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Use value to match with identifer in /raw-data/organigram/organizational-units.json.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "theme": [
                {
                    "fieldInPrimarySource": "StudienID",
                    "locationInPrimarySource": "projekt_und_studienverwaltung.csv",
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["7202001", "7202002", "7202003", "7202004"],
                            "setValues": [
                                Theme[
                                    "NON_COMMUNICABLE_DISEASES_AND_HEALTH_SURVEILLANCE"
                                ]
                            ],
                            "rule": None,
                        },
                        {
                            "forValues": None,
                            "setValues": [
                                Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]
                            ],
                            "rule": 'For all other StudienID set value as mentioned below in "setValues".',
                        },
                    ],
                    "comment": None,
                }
            ],
            "unitInCharge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Use value to match with identifier in /raw-data/organigram/organizational-units.json.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "language": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [Language["GERMAN"]],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "resourceCreationMethod": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                ResourceCreationMethod["STUDIES_SURVEYS_AND_INTERVIEWS"]
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "resourceTypeGeneral": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [ResourceTypeGeneral["DATA_COLLECTION"]],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "rights": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "value": "Gesundheitsdaten",
                                    "language": "de",
                                }
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "spatial": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "Deutschland", "language": "de"}],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "stateOfDataProcessing": [],
        },
        ExtractedResource,
    )

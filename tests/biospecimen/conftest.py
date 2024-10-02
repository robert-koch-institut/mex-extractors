from typing import Any

import pytest

from mex.common.models import ExtractedPerson
from mex.common.types import (
    AccessRestriction,
    AnonymizationPseudonymization,
    Identifier,
    Language,
    PersonalData,
    ResourceCreationMethod,
    ResourceTypeGeneral,
    Theme,
)
from mex.extractors.biospecimen.models.source import BiospecimenResource


@pytest.fixture
def biospecimen_resources() -> list[BiospecimenResource]:
    """Return a dummy biospecimen resource for testing."""
    return [
        BiospecimenResource(
            offizieller_titel_der_probensammlung=["test_titel"],
            beschreibung=["Testbeschreibung"],
            schlagworte=["Testschlagwort 1, Testschlagwort 2"],
            methoden=["Testmethode"],
            zeitlicher_bezug=["2021-09 bis 2021-10"],
            rechte="Testrechte",
            studienbezug=["1234567"],
            alternativer_titel="alternativer Testitel",
            anonymisiert_pseudonymisiert="pseudonymisiert",
            externe_partner="esterner Testpartner",
            id_loinc=["12345-6"],
            id_mesh_begriff=["D123"],
            kontakt=["test_person@email.de"],
            methodenbeschreibung=["Testmethodenbeschreibung"],
            mitwirkende_fachabteilung="mitwirkende Testabteilung",
            mitwirkende_personen="mitwirkende Testperson",
            raeumlicher_bezug=["räumlicher Testbezug"],
            ressourcentyp_allgemein="allgemeiner Testtyp",
            ressourcentyp_speziell=["spezieller Testtyp"],
            sheet_name="Probe1",
            thema=["https://mex.rki.de/item/theme-1"],
            tools_instrumente_oder_apparate="Testtool",
            verantwortliche_fachabteilung="PARENT Dept.",
            verwandte_publikation_doi="testverwandedoi",
            verwandte_publikation_titel="testverwandtepublikation",
            vorhandene_anzahl_der_proben="Testanzahl",
            weiterfuehrende_dokumentation_titel="Testdokutitel",
            weiterfuehrende_dokumentation_url_oder_dateipfad="Testdokupfad",
            zugriffsbeschraenkung="Testbeschränkung",
        )
    ]


@pytest.fixture
def mex_persons() -> list[ExtractedPerson]:
    """Mock and extracted person."""
    return [
        ExtractedPerson(
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="test_id",
            email=["test_person@email.de"],
            familyName=["Müller"],
            fullName=["Müller, Marie"],
            givenName=["Marie"],
        )
    ]


@pytest.fixture
def resource_mapping() -> dict[str, Any]:
    """Mock resource mapping."""
    return {
        "accessRestriction": [
            {
                "fieldInPrimarySource": "Zugriffsbeschränkung",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": ["offen"],
                        "setValues": [AccessRestriction["OPEN"]],
                        "rule": None,
                    },
                    {
                        "forValues": ["restriktiv"],
                        "setValues": [AccessRestriction["RESTRICTED"]],
                        "rule": None,
                    },
                ],
                "comment": None,
            }
        ],
        "accrualPeriodicity": None,
        "created": None,
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
        "theme": [
            {
                "fieldInPrimarySource": "Thema",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": ["Gesundheitsberichterstattung", "Labor"],
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [
                            Theme["NON_COMMUNICABLE_DISEASES_AND_HEALTH_SURVEILLANCE"]
                        ],
                        "rule": "Set the value given in setValues as default for all values given in the primary source. Except the the value mentioned below in forValues.",
                    },
                    {
                        "forValues": ["Infektionskrankheiten"],
                        "setValues": [Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]],
                        "rule": None,
                    },
                ],
                "comment": None,
            }
        ],
        "anonymizationPseudonymization": [
            {
                "fieldInPrimarySource": "anonymisiert / pseudonymisiert",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": ["anonymisiert"],
                        "setValues": [AnonymizationPseudonymization["ANONYMIZED"]],
                        "rule": None,
                    },
                    {
                        "forValues": ["pseudonomysiert"],
                        "setValues": [AnonymizationPseudonymization["PSEUDONYMIZED"]],
                        "rule": None,
                    },
                    {
                        "forValues": ["beides"],
                        "setValues": [
                            AnonymizationPseudonymization["ANONYMIZED"],
                            AnonymizationPseudonymization["PSEUDONYMIZED"],
                        ],
                        "rule": None,
                    },
                ],
                "comment": None,
            }
        ],
        "conformsTo": [
            {
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {"forValues": None, "setValues": ["LOINC"], "rule": None}
                ],
                "comment": None,
            }
        ],
        "hasLegalBasis": [
            {
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [{"value": "DSGVO", "language": "de"}],
                        "rule": None,
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
                    {"forValues": None, "setValues": [Language["GERMAN"]], "rule": None}
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
                            ResourceCreationMethod["STUDIES_SURVEYS_AND_INTERVIEWS"],
                            ResourceCreationMethod["LABORATORY_TESTS"],
                        ],
                        "rule": None,
                    }
                ],
                "comment": None,
            }
        ],
        "resourceTypeGeneral": [
            {
                "fieldInPrimarySource": "Ressourcentyp, allgemein",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": ["Bioproben"],
                "mappingRules": [
                    {
                        "forValues": ["Bioproben"],
                        "setValues": [ResourceTypeGeneral["SAMPLES"]],
                        "rule": None,
                    }
                ],
                "comment": None,
            }
        ],
    }

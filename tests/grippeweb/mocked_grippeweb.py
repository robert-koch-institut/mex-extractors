from typing import Any, TypeVar
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from pytest import MonkeyPatch

from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
    PersonalData,
    ResourceCreationMethod,
    Text,
    TextLanguage,
)
from mex.extractors.grippeweb.connector import GrippewebConnector

ModelT = TypeVar("ModelT", bound=BaseModel)


@pytest.fixture
def extracted_mex_functional_units_grippeweb() -> (
    dict[str, MergedContactPointIdentifier]
):
    return {"contactc@rki.de": MergedContactPointIdentifier.generate(42)}


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> (
    dict[str, MergedOrganizationalUnitIdentifier]
):
    """Mock unit stable target ids."""
    return {"C1": MergedOrganizationalUnitIdentifier.generate(seed=44)}


@pytest.fixture
def extracted_mex_persons_grippeweb() -> list[ExtractedPerson]:
    """Return an extracted person with static dummy values."""
    return [
        ExtractedPerson(
            email=["test_person@email.de"],
            familyName="Contact",
            givenName="Carla",
            fullName="Contact, Carla",
            identifierInPrimarySource="Carla",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=40),
        )
    ]


@pytest.fixture
def grippeweb_organization_ids_by_query_string() -> (
    dict[str, MergedOrganizationIdentifier]
):
    return {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(42)}


@pytest.fixture
def grippeweb_access_platform() -> dict[str, Any]:
    return {
        "hadPrimarySource": [
            {
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": None,
                        "rule": "Assign 'stable target id' of primary source with identifier 'grippeweb' in /raw-data/primary-sources/primary-sources.json.",
                    }
                ],
                "comment": None,
            }
        ],
        "identifierInPrimarySource": [
            {
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": ["primary-source"],
                        "rule": "use value as indicated.",
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
                        "forValues": ["test_person@email.de"],
                        "setValues": None,
                        "rule": "Match value using ldap extractor",
                    }
                ],
                "comment": None,
            }
        ],
        "technicalAccessibility": [
            {
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [
                            "https://mex.rki.de/item/technical-accessibility-1"
                        ],
                        "rule": None,
                    }
                ],
                "comment": None,
            }
        ],
        "title": [
            {
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [{"value": "primary-source", "language": "en"}],
                        "rule": None,
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
                        "rule": "Match value using organigram extractor.",
                    }
                ],
                "comment": None,
            }
        ],
    }


@pytest.fixture
def grippeweb_resource_mappings() -> list[dict[str, Any]]:
    return [
        {
            "hadPrimarySource": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "Assign 'stable target id' of primary source with identifier 'grippeweb' in /raw-data/primary-sources/primary-sources.json.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "identifierInPrimarySource": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": ["grippeweb-plus"],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "accessPlatform": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "assign 'stable target id' of item described by mapping/grippeweb/access-platform",
                        }
                    ],
                    "comment": None,
                }
            ],
            "accessRestriction": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                "https://mex.rki.de/item/access-restriction-2"
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "accrualPeriodicity": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": ["https://mex.rki.de/item/frequency-15"],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "anonymizationPseudonymization": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/anonymization-pseudonymization-2",
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
                            "forValues": ["contactc@rki.de"],
                            "setValues": None,
                            "rule": "Match value using ldap extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "contributingUnit": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Match identifer using organigram extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "contributor": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["Carla Contact"],
                            "setValues": None,
                            "rule": "Match values using ldap extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "created": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["2020"], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "creator": None,
            "description": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "value": "Erreger-spezifische Zusatzinformationen",
                                    "language": "de",
                                }
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "distribution": None,
            "documentation": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "RKI Website",
                                    "url": "https://www.rki.de",
                                }
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "externalPartner": None,
            "hasLegalBasis": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "value": "Bei dem Verfahren.",
                                    "language": TextLanguage.DE,
                                }
                            ],
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
            "icd10code": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["J00-J99"], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "instrumentToolOrApparatus": None,
            "isPartOf": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "Assign 'stable target id' of item described by mapping/../grippeweb/resource_grippeweb",
                        }
                    ],
                    "comment": None,
                }
            ],
            "keyword": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "Surveillance", "language": "de"}],
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
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/language-1",
                            "rule": None,
                        }
                    ],
                    "comment": "Deutsch",
                }
            ],
            "meshId": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                "http://id.nlm.nih.gov/mesh/D012140",
                                "http://id.nlm.nih.gov/mesh/D012141",
                                "http://id.nlm.nih.gov/mesh/D007251",
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "method": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {"value": "Selbstabstriche", "language": "de"}
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "methodDescription": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "Stichprobe", "language": "de"}],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "minTypicalAge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": [0], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "populationCoverage": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {"value": "Alle Personen.", "language": TextLanguage.DE}
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "publisher": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["Robert Koch-Institut"],
                            "setValues": None,
                            "rule": "Match value with organization item using wikidata extractor.",
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
                            "setValues": [ResourceCreationMethod["SURVEILLANCE"]],
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
                            "setValues": "https://mex.rki.de/item/resource-type-general-13",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "resourceTypeSpecific": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "Nasenabstrich", "language": "de"}],
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
                            "setValues": [{"value": "Die Daten", "language": "de"}],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "sizeOfDataBasis": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": ["500 Teilnehmende (Stand Mai 2023)"],
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
            "stateOfDataProcessing": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/data-processing-state-1",
                            "rule": None,
                        }
                    ],
                    "comment": "Rohdaten",
                }
            ],
            "temporal": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["seit 2020"], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "theme": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/theme-11",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "title": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {"value": "GrippeWeb-Plus", "language": "de"}
                            ],
                            "rule": None,
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
                            "rule": "Match value using organigram extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "wasGeneratedBy": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Match value with corresponding identifierInPrimarySource.",
                        }
                    ],
                    "comment": None,
                }
            ],
        },
        {
            "hadPrimarySource": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "Assign 'stable target id' of primary source with identifier 'grippeweb' in /raw-data/primary-sources/primary-sources.json.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "identifierInPrimarySource": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": ["grippeweb"],
                            "rule": "Use value as indicated.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "accessPlatform": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "Assign 'stable target id' of item described by mappings/../grippeweb/access-platform",
                        }
                    ],
                    "comment": None,
                }
            ],
            "accessRestriction": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/access-restriction-2",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "accrualPeriodicity": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/frequency-15",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "alternativeTitle": None,
            "anonymizationPseudonymization": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/anonymization-pseudonymization-2",
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
                            "forValues": ["contactc@rki.de"],
                            "setValues": None,
                            "rule": "Match value by using ldap extract extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "contributingUnit": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["C1"],
                            "setValues": None,
                            "rule": "Match value with identifier using organigram extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "contributor": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["Carla Contact"],
                            "setValues": None,
                            "rule": "Match values using ldap extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "created": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["2011"], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "creator": None,
            "description": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "GrippeWeb", "language": "de"}],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "distribution": None,
            "documentation": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "RKI Website",
                                    "url": "https://www.rki.de",
                                }
                            ],
                            "rule": None,
                        }
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
                            "setValues": [
                                {
                                    "value": "Bei dem Verfahren.",
                                    "language": TextLanguage.DE,
                                }
                            ],
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
            "icd10code": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["J00-J99"], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "instrumentToolOrApparatus": None,
            "isPartOf": None,
            "keyword": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {"value": "Citizen Science", "language": "en"}
                            ],
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
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/language-1",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "license": None,
            "loincId": None,
            "meshId": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": ["http://id.nlm.nih.gov/mesh/D012140"],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "method": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {"value": "Online-Befragung", "language": "de"}
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "methodDescription": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "value": "Online-Surveillanceintrument",
                                    "language": "de",
                                }
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "minTypicalAge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": [0], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "populationCoverage": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {"value": "Alle Personen.", "language": TextLanguage.DE}
                            ],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "modified": None,
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "publisher": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": ["Robert Koch-Institut"],
                            "setValues": None,
                            "rule": "Match value with organization item using wikidata extractor.",
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
                            "setValues": [ResourceCreationMethod["SURVEILLANCE"]],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "qualityInformation": None,
            "resourceTypeGeneral": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/resource-type-general-13",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "resourceTypeSpecific": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "value": "bevölkerungsbasierte Surveillancedaten",
                                    "language": "de",
                                }
                            ],
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
                            "setValues": [{"value": "Verfahren", "language": "de"}],
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "sizeOfDataBasis": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["Meldungen"], "rule": None}
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
            "stateOfDataProcessing": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/data-processing-state-1",
                            "rule": None,
                        }
                    ],
                    "comment": None,
                }
            ],
            "temporal": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {"forValues": None, "setValues": ["seit 2011"], "rule": None}
                    ],
                    "comment": None,
                }
            ],
            "theme": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": "https://mex.rki.de/item/theme-11",
                            "rule": None,
                        },
                    ],
                    "comment": None,
                }
            ],
            "title": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "GrippeWeb", "language": "de"}],
                            "rule": None,
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
                            "rule": "Match value using organigram extractor.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "identifier": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "Assign identifier.",
                        }
                    ],
                    "comment": None,
                }
            ],
            "stableTargetId": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": None,
                            "rule": "Assign 'stable target id' of merged item.",
                        }
                    ],
                    "comment": None,
                }
            ],
        },
    ]


@pytest.fixture
def grippeweb_extracted_access_platform(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> ExtractedAccessPlatform:
    return ExtractedAccessPlatform(
        hadPrimarySource=extracted_primary_sources["grippeweb"].stableTargetId,
        identifierInPrimarySource="primary-source",
        contact=[MergedContactPointIdentifier.generate(seed=234)],
        technicalAccessibility="https://mex.rki.de/item/technical-accessibility-1",
        title=[Text(value="primary-source", language="en")],
        unitInCharge=[MergedOrganizationalUnitIdentifier.generate(seed=235)],
    )


@pytest.fixture
def mocked_grippeweb(
    mocked_grippeweb_sql_tables: dict[str, dict[str, list[str | None]]],
    monkeypatch: MonkeyPatch,
) -> None:
    """Mock grippeweb connector."""

    def mocked_init(self: GrippewebConnector) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = ["mocked"]
        self._connection = MagicMock()
        self._connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(GrippewebConnector, "__init__", mocked_init)

    monkeypatch.setattr(
        GrippewebConnector,
        "parse_columns_by_column_name",
        lambda self, model: mocked_grippeweb_sql_tables[model],
    )


@pytest.fixture
def mocked_grippeweb_sql_tables() -> dict[str, dict[str, list[str | None]]]:
    return {
        "vActualQuestion": {
            "Id": ["AAA", "BBB"],
            "StartedOn": ["2023-11-01 00:00:00.0000000", "2023-12-01 00:00:00.0000000"],
            "FinishedOn": [
                "2023-12-01 00:00:00.0000000",
                "2024-01-01 00:00:00.0000000",
            ],
            "RepeatAfterDays": ["1", "2"],
        },
        "vWeeklyResponsesMEx": {
            "GuidTeilnehmer": [None, None],
            "Haushalt_Registrierer": [None, None],
        },
        "vMasterDataMEx": {
            "GuidTeilnehmer": [None, None],
            "Haushalt_Registrierer": [None, None],
        },
    }


@pytest.fixture
def grippeweb_variable_group() -> dict[str, Any]:
    return {
        "label": [
            {
                "fieldInPrimarySource": "label of the SQL-table",
                "locationInPrimarySource": "Tabellen",
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": ["MEx.vActualQuestion"],
                        "setValues": [
                            {"value": "Additional Questions", "language": "en"}
                        ],
                        "rule": None,
                    },
                    {
                        "forValues": ["MEx.vMasterDataMEx"],
                        "setValues": [{"value": "Master Data", "language": None}],
                        "rule": None,
                    },
                    {
                        "forValues": ["MEx.vWeeklyResponsesMEx"],
                        "setValues": [{"value": "Weekly Responses", "language": "en"}],
                        "rule": None,
                    },
                ],
                "comment": None,
            }
        ],
    }


@pytest.fixture
def grippeweb_variable() -> dict[str, Any]:
    return {
        "valueSet": [
            {
                "fieldInPrimarySource": "Id",
                "locationInPrimarySource": "vActualQuestion",
                "examplesInPrimarySource": ["NULL", "Diverse", "Female"],
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": None,
                        "rule": "SELECT DISTINCT [fieldInPrimarySource] FROM [locationInPrimarySource]",
                    }
                ],
                "comment": None,
            }
        ],
    }


@pytest.fixture
def grippeweb_extracted_resource_dict(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> dict[str, ExtractedResource]:
    return {
        "grippeweb": ExtractedResource(
            hadPrimarySource=extracted_primary_sources["grippeweb"].stableTargetId,
            identifierInPrimarySource="grippeweb",
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-15",
            contact=[MergedContactPointIdentifier.generate(42)],
            temporal="seit 2011",
            theme=["https://mex.rki.de/item/theme-11"],
            title=[Text(value="GrippeWeb", language="de")],
            anonymizationPseudonymization=[
                "https://mex.rki.de/item/anonymization-pseudonymization-2"
            ],
            description=[Text(value="GrippeWeb", language="de")],
            icd10code=["J00-J99"],
            keyword=[Text(value="Citizen Science", language="en")],
            language=["https://mex.rki.de/item/language-1"],
            meshId=["http://id.nlm.nih.gov/mesh/D012140"],
            method=[Text(value="Online-Befragung", language="de")],
            methodDescription=[
                Text(value="Online-Surveillanceintrument", language="de")
            ],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
            resourceTypeSpecific=[
                Text(value="bevölkerungsbasierte Surveillancedaten", language="de")
            ],
            rights=[Text(value="Verfahren", language="de")],
            stateOfDataProcessing=["https://mex.rki.de/item/data-processing-state-1"],
            unitInCharge=[MergedOrganizationalUnitIdentifier.generate(42)],
            entityType="ExtractedResource",
        )
    }


@pytest.fixture
def extracted_variable_groups(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    grippeweb_extracted_resource_dict: dict[str, ExtractedResource],
) -> list[ExtractedVariableGroup]:
    return [
        ExtractedVariableGroup(
            hadPrimarySource=extracted_primary_sources["grippeweb"].stableTargetId,
            identifierInPrimarySource="vActualQuestion",
            containedBy=[grippeweb_extracted_resource_dict["grippeweb"].stableTargetId],
            label=[Text(value="Additional Questions", language="en")],
            entityType="ExtractedVariableGroup",
        )
    ]

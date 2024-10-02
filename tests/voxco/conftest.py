from typing import Any, TypeVar

import pytest
from pydantic import BaseModel

from mex.common.models import ExtractedActivity, ExtractedPerson, ExtractedResource
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
    Text,
)
from mex.extractors.voxco.model import VoxcoVariable

ModelT = TypeVar("ModelT", bound=BaseModel)


@pytest.fixture
def extracted_mex_functional_units_voxco() -> dict[str, MergedContactPointIdentifier]:
    return {"contactc@rki.de": MergedContactPointIdentifier.generate(42)}


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> (
    dict[str, MergedOrganizationalUnitIdentifier]
):
    """Mock unit stable target ids."""
    return {"C1": MergedOrganizationalUnitIdentifier.generate(seed=44)}


@pytest.fixture
def extracted_mex_persons_voxco() -> list[ExtractedPerson]:
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
def organization_stable_target_id_by_query_voxco() -> (
    dict[str, MergedOrganizationIdentifier]
):
    return {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(42)}


@pytest.fixture
def voxco_resource_mappings() -> list[dict[str, Any]]:
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
                            "rule": "Assign 'stable target id' of primary source with identifier 'voxco' in /raw-data/primary-sources/primary-sources.json.",
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
                            "setValues": ["voxco-plus"],
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
                            "rule": "assign 'stable target id' of item described by mapping/voxco/access-platform",
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
                            "forValues": ["Person Test", "test_person@email.de"],
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
            "externalPartner": [
                {"mappingRules": [{"forValues": ["Robert Koch-Institut"]}]}
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
                            "rule": "Assign 'stable target id' of item described by mapping/../voxco/resource_voxco",
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
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [
                                {
                                    "language": "en",
                                    "title": "Feasibility study",
                                    "url": "https://doi.org/10.25646/11292",
                                }
                            ],
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
                            "setValues": "https://mex.rki.de/item/resource-creation-method-2",
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
                            "setValues": "https://mex.rki.de/item/resource-type-general-15",
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
            "qualityInformation": [
                {
                    "fieldInPrimarySource": "n/a",
                    "locationInPrimarySource": None,
                    "examplesInPrimarySource": None,
                    "mappingRules": [
                        {
                            "forValues": None,
                            "setValues": [{"value": "description", "language": "de"}],
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
                            "setValues": "https://mex.rki.de/item/theme-37",
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
                            "setValues": [{"value": "voxco-Plus", "language": "de"}],
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
                            "forValues": ["2022-006"],
                            "setValues": None,
                            "rule": "Match value with corresponding identifierInPrimarySource.",
                        }
                    ],
                    "comment": None,
                }
            ],
        },
    ]


@pytest.fixture
def extracted_voxco_resources() -> dict[str, ExtractedResource]:
    return {
        "voxco-plus": ExtractedResource(
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(21),
            identifierInPrimarySource="voxco-plus",
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            theme=["https://mex.rki.de/item/theme-37"],
            title=[Text(value="voxco-Plus", language="de")],
            anonymizationPseudonymization=[
                "https://mex.rki.de/item/anonymization-pseudonymization-2"
            ],
            contact=[MergedOrganizationalUnitIdentifier.generate(22)],
            description=[
                Text(value="Erreger-spezifische Zusatzinformationen", language="de")
            ],
            keyword=[Text(value="Surveillance", language="de")],
            language=["https://mex.rki.de/item/language-1"],
            meshId=[
                "http://id.nlm.nih.gov/mesh/D012140",
                "http://id.nlm.nih.gov/mesh/D012141",
                "http://id.nlm.nih.gov/mesh/D007251",
            ],
            method=[Text(value="Selbstabstriche", language="de")],
            qualityInformation=[Text(value="description", language="de")],
            resourceCreationMethod=[
                "https://mex.rki.de/item/resource-creation-method-2"
            ],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-15"],
            resourceTypeSpecific=[Text(value="Nasenabstrich", language="de")],
            rights=[Text(value="Die Daten", language="de")],
            spatial=[Text(value="Deutschland", language="de")],
            entityType="ExtractedResource",
            unitInCharge=[MergedOrganizationalUnitIdentifier.generate(23)],
        )
    }


@pytest.fixture
def voxco_variables() -> dict[str, list[VoxcoVariable]]:
    return {
        "project_voxco-plus": [
            VoxcoVariable(
                Id=50614,
                DataType="Text",
                Type="Discrete",
                QuestionText="Monat",
                Choices=[
                    "@{Code=1; Text=Januar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
                    "@{Code=1; Text=Februar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
                ],
                Text="Tag",
            )
        ]
    }


@pytest.fixture
def extracted_international_projects_activities() -> list[ExtractedActivity]:
    return [
        ExtractedActivity(
            contact=MergedOrganizationalUnitIdentifier.generate(30),
            responsibleUnit=MergedOrganizationalUnitIdentifier.generate(32),
            title=[Text(value="title", language="de")],
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(31),
            identifierInPrimarySource="2022-006",
        )
    ]

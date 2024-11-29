import pytest
from numpy import nan

from mex.common.models import (
    ExtractedResource,
)
from mex.common.types import (
    AccessRestriction,
    Language,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    Text,
)
from mex.extractors.mapping.transform import transform_mapping_data_to_models
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.odk.model import ODKData


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> (
    dict[str, MergedOrganizationalUnitIdentifier]
):
    """Mock unit stable target ids."""
    return {"C1": MergedOrganizationalUnitIdentifier.generate(seed=44)}


@pytest.fixture
def odk_resource_mappings() -> list[AnyMappingModel]:
    """Mocked odk resource mappings."""
    return transform_mapping_data_to_models(
        [
            {
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
                "hadPrimarySource": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": None,
                                "rule": "Assign 'stable target id'",
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
                                "setValues": ["test_raw_data"],
                                "rule": "Use value as it is.",
                            }
                        ],
                        "comment": "identifierInPrimarySource also for the corresponding activity extracted from international projects",
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
                "accessPlatform": [],
                "accessRestriction": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": [AccessRestriction["RESTRICTED"]],
                                "rule": "Use value as it is.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "accrualPeriodicity": [],
                "alternativeTitle": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": [
                                    {"value": "dolor", "language": "en"},
                                    {"value": "sit", "language": "de"},
                                ],
                                "rule": "Set values as indicated.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "anonymizationPseudonymization": [],
                "contact": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": ["C1"],
                                "setValues": None,
                                "rule": "Match value with corresponding identifier using organigram extractor.",
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
                                "rule": "Match value with corresponding identifier using organigram extractor.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "description": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": [{"value": "amet", "language": "en"}],
                                "rule": "Set value as indicated.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "distribution": [],
                "documentation": [],
                "externalPartner": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": [
                                    "consetetur",
                                    "invidunt",
                                    " sadipscing  ",
                                ],
                                "setValues": None,
                                "rule": "Use value",
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
                                    {"value": "Informed consent", "language": "en"}
                                ],
                                "rule": "Set value as indicated.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "icd10code": [],
                "instrumentToolOrApparatus": [],
                "isPartOf": [],
                "keyword": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": [
                                    {"value": "elitr", "language": None},
                                    {"value": "sed", "language": "en"},
                                    {"value": "diam", "language": "en"},
                                ],
                                "rule": "Set values as indicated.",
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
                                "setValues": [Language["ENGLISH"]],
                                "rule": "Set value as indicated.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "license": [],
                "loincId": [],
                "meshId": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": ["http://id.nlm.nih.gov/mesh/D000086382"],
                                "rule": "Set values as indicated.",
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
                                    {"value": "nonumy", "language": "en"},
                                    {"value": "eirmod", "language": "de"},
                                ],
                                "rule": "Set values as indicated.",
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
                                "setValues": [{"value": "tempor", "language": "en"}],
                                "rule": "Set value as indicated.",
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
                                "forValues": ["invidunt", "consetetur"],
                                "setValues": None,
                                "rule": "Use value to match with wikidata extractor.",
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
                                    "https://mex.rki.de/item/resource-creation-method-2"
                                ],
                                "rule": "Set value as indicated.",
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
                                "setValues": [
                                    "https://mex.rki.de/item/resource-type-general-15"
                                ],
                                "rule": "Set value as indicated.",
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
                                "setValues": [{"value": "tempor", "language": "en"}],
                                "rule": "Set value as indicated.",
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
                                "setValues": [{"value": "ut labore", "language": "de"}],
                                "rule": "Set value as indicated.",
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
                                "setValues": ["et dolore"],
                                "rule": "Set value as indicated",
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
                                "setValues": [
                                    {"value": "magna", "language": "de"},
                                    {"value": "magna", "language": "en"},
                                ],
                                "rule": "Set values as indicated.",
                            }
                        ],
                        "comment": None,
                    }
                ],
                "stateOfDataProcessing": [],
                "temporal": [
                    {
                        "fieldInPrimarySource": "n/a",
                        "locationInPrimarySource": None,
                        "examplesInPrimarySource": None,
                        "mappingRules": [
                            {
                                "forValues": None,
                                "setValues": ["2021-07-27 - 2021-12-31"],
                                "rule": "Set value as indicated.",
                            }
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
                                "setValues": [
                                    "https://mex.rki.de/item/theme-11",
                                    "https://mex.rki.de/item/theme-37",
                                ],
                                "rule": "Set values as indicated.",
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
                                    {"value": "aliquyam", "language": "en"},
                                    {"value": "erat", "language": "de"},
                                ],
                                "rule": "Set values as indicated.",
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
                                "rule": "Match value with corresponding identifier using organigram extractor.",
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
                                "forValues": ["0000-1000"],
                                "setValues": None,
                                "rule": "Use value",
                            }
                        ],
                        "comment": None,
                    }
                ],
            }
        ],
        ExtractedResource,
    )


@pytest.fixture
def external_partner_and_publisher_by_label() -> (
    dict[str, MergedOrganizationIdentifier]
):
    """Mocked external partner and publisher dict for OrganizationIDs."""
    return {
        "invidunt": MergedOrganizationIdentifier.generate(42),
        "consetetur": MergedOrganizationIdentifier.generate(43),
    }


@pytest.fixture
def extracted_resources_odk() -> list[ExtractedResource]:
    """Mocked odk mex resources."""
    return [
        ExtractedResource(
            hadPrimarySource="00000000000000",
            identifierInPrimarySource="test_raw_data",
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            alternativeTitle=[
                Text(value="dolor", language="en"),
                Text(value="sit", language="de"),
            ],
            contact=[MergedOrganizationalUnitIdentifier.generate(42)],
            contributingUnit=[MergedOrganizationalUnitIdentifier.generate(43)],
            description=[Text(value="amet", language="en")],
            externalPartner=[MergedOrganizationalUnitIdentifier.generate(44)],
            keyword=[
                Text(value="elitr", language=None),
                Text(value="sed", language="en"),
                Text(value="diam", language="en"),
            ],
            language=["https://mex.rki.de/item/language-2"],
            meshId=["http://id.nlm.nih.gov/mesh/D000086382"],
            method=[
                Text(value="nonumy", language="en"),
                Text(value="eirmod", language="de"),
            ],
            methodDescription=[Text(value="tempor", language="en")],
            publisher=["bFQoRhcVH5DHU6"],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-15"],
            rights=[Text(value="ut labore", language="de")],
            sizeOfDataBasis="et dolore",
            spatial=[
                Text(value="magna", language="de"),
                Text(value="magna", language="en"),
            ],
            stateOfDataProcessing=[],
            temporal="2021-07-27 - 2021-12-31",
            theme=[
                "https://mex.rki.de/item/theme-11",
                "https://mex.rki.de/item/theme-37",
            ],
            title=[
                Text(value="aliquyam", language="en"),
                Text(value="erat", language="de"),
            ],
            unitInCharge=[MergedOrganizationalUnitIdentifier.generate(45)],
            entityType="ExtractedResource",
        )
    ]


@pytest.fixture
def odk_raw_data() -> list[ODKData]:
    """Mocked odk raw data."""
    return [
        ODKData(
            file_name="test_raw_data.xlsx",
            hint={
                "hint": [
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    "*(-88 = don't know, -99 = refused to answer)*",
                    "*(-88 = don't know, -99 = refused to answer)*",
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                    nan,
                ]
            },
            label_choices={
                "label::English (en)": [
                    nan,
                    "I AGREE with the above statements and wish to take part in the survey",
                    "I do NOT AGREE to take part in the survey",
                    nan,
                    "Yes",
                    "No",
                    nan,
                    "Yes",
                    "No",
                    "Don't know",
                    "Refused to answer",
                    nan,
                    "Head of household",
                    "Wife, husband, partner",
                    "Son or daughter",
                ],
                "label::Otjiherero (hz)": [
                    nan,
                    "Ami ME ITAVERE komaheya nge ri kombanda mba nu otji me raisa kutja mbi nonḓero okukara norupa mongonḓononeno.",
                    "Ami HI NOKUITAVERA okukara norupa mongonḓononeno.",
                    nan,
                    "Ii",
                    "Kako",
                    nan,
                    "Ii",
                    "Kako",
                    "Ke nokutjiwa",
                    "Ma panḓa okuzira",
                    nan,
                    "Otjiuru tjeṱunḓu",
                    "Omukazendu we, omurumendu we, otjiyambura tje",
                    "Omuatje we omuzandu poo omukazona",
                ],
            },
            label_survey={
                "label::English (en)": [
                    nan,
                    "Store username of interviewer.",
                    "Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "Introduction of study to gatekeeper",
                    nan,
                    "**Verbal consent**",
                    nan,
                    nan,
                    "Are you",
                    "*(Interviewer: End of Interview.)*",
                    "Taken together",
                    "How many",
                    "*(Interviewer: End of interview, no adult household members)*",
                    "Thank you for providing this basic information.",
                    "*(Interviewer: End of interview.)*",
                    "Introduction of study to gatekeeper",
                    nan,
                    "Selection of respondent",
                    "**Verbal consent**",
                    "*(Interviewer: No more adult household members. End of interview.)*",
                    "Are you currently 18 years old or older?",
                    "Selection of respondent",
                ],
                "label::Otjiherero (hz)": [
                    nan,
                    "Store username of interviewer.",
                    "Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "Introduction of study to gatekeeper",
                    nan,
                    "**Omaitaverero wokotjinyo**",
                    nan,
                    nan,
                    "Ove moyenene okunyamukura omapuriro inga?",
                    "*(Interviewer: End of Interview.)*",
                    "Tji wa twa kumwe",
                    "Ovandu vengapi",
                    "*(Interviewer:",
                    "Okuhepa tjinene",
                    "*(Interviewer:",
                    nan,
                    nan,
                    "Selection of respondent",
                    "**Omaitaverero wokotjinyo**",
                    "*(Interviewer: No more adult household members. End of interview.)*",
                    "Una ozombura 18 nokombanda?",
                    nan,
                ],
            },
            list_name_choices=[
                nan,
                "consent",
                "consent",
                nan,
                "yes_no",
                "yes_no",
                nan,
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                nan,
                "relationship_to_head",
                "relationship_to_head",
                "relationship_to_head",
            ],
            name_choices=[
                nan,
                "consent",
                "consent",
                nan,
                "yes_no",
                "yes_no",
                nan,
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                nan,
                "relationship_to_head",
                "relationship_to_head",
                "relationship_to_head",
            ],
            name_survey=[
                "start",
                "username",
                "confirmed_username",
                "region",
                "constituency",
                "household_id",
                "gatekeeper",
                nan,
                "consent_gatekeeper",
                nan,
                nan,
                "consent_basic_questions",
                "end_of_interview_1",
                "NR1",
                "NR2",
                "NR3end",
                "consent_gatekeeper_2",
                "end_of_interview_3",
                "gatekeeper",
                nan,
                "selection",
                "consent_respondent",
                "end_of_interview_2",
                "age_verification",
                "selection",
            ],
            type_survey=[
                "start",
                "username",
                "text",
                "select_one region",
                "select_one constituency",
                "text",
                "begin_group",
                nan,
                "select_one consent",
                nan,
                nan,
                "select_one yes_no",
                "note",
                "integer",
                "integer",
                "note",
                "select_one yes_no",
                "note",
                "end_group",
                nan,
                "begin_group",
                "select_one consent",
                "note",
                "select_one yes_no",
                "end_group",
            ],
        )
    ]


@pytest.fixture
def odk_table_groups() -> dict[str, list[dict[str, str | float]]]:
    """Mocked odk variable groups."""
    return {
        "gatekeeper": [
            {
                "type": "begin_group",
                "name": "gatekeeper",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Introduction of study to gatekeeper",
                "label::Otjiherero (hz)": "Introduction of study to gatekeeper",
                "hint": nan,
            },
            {
                "type": "select_one consent",
                "name": "consent_gatekeeper",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "**Verbal consent**",
                "label::Otjiherero (hz)": "**Omaitaverero wokotjinyo**",
                "hint": nan,
            },
            {
                "type": "select_one yes_no",
                "name": "consent_basic_questions",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Are you",
                "label::Otjiherero (hz)": "Ove moyenene okunyamukura omapuriro inga?",
                "hint": nan,
            },
            {
                "type": "integer",
                "name": "NR1",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Taken together",
                "label::Otjiherero (hz)": "Tji wa twa kumwe",
                "hint": "*(-88 = don't know, -99 = refused to answer)*",
            },
            {
                "type": "integer",
                "name": "NR2",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "How many",
                "label::Otjiherero (hz)": "Ovandu vengapi",
                "hint": "*(-88 = don't know, -99 = refused to answer)*",
            },
            {
                "type": "select_one yes_no",
                "name": "consent_gatekeeper_2",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Thank you for providing this basic information.",
                "label::Otjiherero (hz)": "Okuhepa tjinene",
                "hint": nan,
            },
        ],
        "selection": [
            {
                "type": "begin_group",
                "name": "selection",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Selection of respondent",
                "label::Otjiherero (hz)": "Selection of respondent",
                "hint": nan,
            },
            {
                "type": "select_one consent",
                "name": "consent_respondent",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "**Verbal consent**",
                "label::Otjiherero (hz)": "**Omaitaverero wokotjinyo**",
                "hint": nan,
            },
            {
                "type": "select_one yes_no",
                "name": "age_verification",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Are you currently 18 years old or older?",
                "label::Otjiherero (hz)": "Una ozombura 18 nokombanda?",
                "hint": nan,
            },
        ],
    }

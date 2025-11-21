import pytest

from mex.common.models import (
    ExtractedActivity,
    ExtractedResource,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import (
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    Text,
)
from mex.extractors.odk.model import ODKData
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def international_projects_extracted_activities() -> list[ExtractedActivity]:
    return [
        ExtractedActivity(
            hadPrimarySource="fSwk5o6nXHVMdFuPHH0hRk",
            identifierInPrimarySource="0000-1000",
            contact=["bFQoRhcVH5DHUU", "bFQoRhcVH5DHUL"],
            responsibleUnit=["bFQoRhcVH5DHUL"],
            title="This is a test project full title",
            activityType=[
                "https://mex.rki.de/item/activity-type-3",
                "https://mex.rki.de/item/activity-type-1",
            ],
            alternativeTitle="testAAbr",
            end="2021-12-31",
            externalAssociate=["bFQoRhcVH5DHU8"],
            funderOrCommissioner=["bFQoRhcVH5DHU8"],
            involvedPerson=["bFQoRhcVH5DHUU"],
            involvedUnit=["bFQoRhcVH5DHUL"],
            shortName="testAAbr",
            start="2021-07-27",
            theme=["https://mex.rki.de/item/theme-37"],
            website=[],
        ),
        ExtractedActivity(
            hadPrimarySource="fSwk5o6nXHVMdFuPHH0hRk",
            identifierInPrimarySource="0000-1001",
            contact=["bFQoRhcVH5DHUU", "bFQoRhcVH5DHUL"],
            responsibleUnit=["bFQoRhcVH5DHUL"],
            title="This is a test project full title 2",
            activityType=[
                "https://mex.rki.de/item/activity-type-3",
                "https://mex.rki.de/item/activity-type-1",
            ],
            alternativeTitle="testAAbr2",
            end="2025-12-31",
            funderOrCommissioner=["bFQoRhcVH5DHU8"],
            fundingProgram=["GHPP2"],
            involvedPerson=["bFQoRhcVH5DHUU"],
            shortName="testAAbr2",
            start="2023-01-01",
            theme=[
                "https://mex.rki.de/item/theme-37",
            ],
        ),
        ExtractedActivity(
            hadPrimarySource="fSwk5o6nXHVMdFuPHH0hRk",
            identifierInPrimarySource="0000-1002",
            contact=["bFQoRhcVH5DHUU", "bFQoRhcVH5DHUL"],
            responsibleUnit=["bFQoRhcVH5DHUL"],
            title="This is a test project full title 4",
            activityType=[
                "https://mex.rki.de/item/activity-type-3",
                "https://mex.rki.de/item/activity-type-1",
            ],
            alternativeTitle="testAAbr3",
            end="2022-12-31",
            funderOrCommissioner=["bFQoRhcVH5DHU8"],
            fundingProgram=["None"],
            involvedPerson=["bFQoRhcVH5DHUU"],
            involvedUnit=["bFQoRhcVH5DHUL"],
            shortName="testAAbr3",
            start="2021-08-01",
            theme=[
                "https://mex.rki.de/item/theme-37",
            ],
            website=[Link(language=None, title=None, url="None")],
        ),
    ]


@pytest.fixture
def unit_stable_target_ids_by_synonym() ->  dict[
        str, list[MergedOrganizationalUnitIdentifier]
        ]:
    """Mock unit stable target ids."""
    return {"C1": [MergedOrganizationalUnitIdentifier.generate(seed=44)]}


@pytest.fixture
def odk_resource_mappings(settings: Settings) -> list[ResourceMapping]:
    """Mocked odk resource mappings."""
    return [
        ResourceMapping.model_validate(
            load_yaml(settings.odk.mapping_path / "resource_mock.yaml")
        )
    ]


@pytest.fixture
def odk_variable_mapping(settings: Settings) -> VariableMapping:
    """Mocked odk variable mappings."""
    return VariableMapping.model_validate(
        load_yaml(settings.odk.mapping_path / "variable.yaml")
    )


@pytest.fixture
def odk_merged_organization_ids_by_str() -> dict[str, MergedOrganizationIdentifier]:
    """Mocked external partner and publisher dict for OrganizationIDs."""
    return {
        "invidunt": MergedOrganizationIdentifier.generate(42),
        "consetetur": MergedOrganizationIdentifier.generate(43),
    }


@pytest.fixture
def odk_extracted_resources() -> list[ExtractedResource]:
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
            label_choices={
                "label::English (en)": [
                    "nan",
                    "I AGREE with the above statements and wish to take part in the survey",
                    "I do NOT AGREE to take part in the survey",
                    "nan",
                    "Yes",
                    "No",
                    "nan",
                    "Yes",
                    "No",
                    "Don't know",
                    "nan",
                    "nan",
                    "Head of household",
                    "Wife, husband, partner",
                    "nan",
                ],
                "label::Otjiherero (hz)": [
                    "nan",
                    "Ami ME ITAVERE komaheya nge ri kombanda mba nu otji me raisa kutja mbi nonḓero okukara norupa mongonḓononeno.",
                    "Ami HI NOKUITAVERA okukara norupa mongonḓononeno.",
                    "nan",
                    "nan",
                    "nan",
                    "nan",
                    "Ii",
                    "Kako",
                    "Ke nokutjiwa",
                    "Ma panḓa okuzira",
                    "nan",
                    "Otjiuru tjeṱunḓu",
                    "Omukazendu we, omurumendu we, otjiyambura tje",
                    "Omuatje we omuzandu poo omukazona",
                ],
            },
            label_survey={
                "label::English (en)": [
                    "nan",
                    "Store username of interviewer.",
                    "Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "Introduction of study to gatekeeper",
                    "nan",
                    "**Verbal consent**",
                    "nan",
                    "nan",
                    "Are you",
                    "*(Interviewer: End of Interview.)*",
                    "Taken together",
                    "How many",
                    "*(Interviewer: End of interview, no adult household members)*",
                    "Thank you for providing this basic information.",
                    "*(Interviewer: End of interview.)*",
                    "Introduction of study to gatekeeper",
                    "nan",
                    "Selection of respondent",
                    "**Verbal consent**",
                    "*(Interviewer: No more adult household members. End of interview.)*",
                    "Are you currently 18 years old or older?",
                    "Selection of respondent",
                ],
                "label::Otjiherero (hz)": [
                    "nan",
                    "Store username of interviewer.",
                    "Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "(*Interviewer:",
                    "Introduction of study to gatekeeper",
                    "nan",
                    "**Omaitaverero wokotjinyo**",
                    "nan",
                    "nan",
                    "Ove moyenene okunyamukura omapuriro inga?",
                    "*(Interviewer: End of Interview.)*",
                    "Tji wa twa kumwe",
                    "Ovandu vengapi",
                    "*(Interviewer:",
                    "Okuhepa tjinene",
                    "*(Interviewer:",
                    "nan",
                    "nan",
                    "Selection of respondent",
                    "**Omaitaverero wokotjinyo**",
                    "*(Interviewer: No more adult household members. End of interview.)*",
                    "Una ozombura 18 nokombanda?",
                    "nan",
                ],
            },
            list_name_choices=[
                "nan",
                "consent",
                "consent",
                "nan",
                "yes_no",
                "yes_no",
                "nan",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "nan",
                "relationship_to_head",
                "relationship_to_head",
                "relationship_to_head",
            ],
            name_choices=[
                "nan",
                "consent",
                "consent",
                "nan",
                "yes_no",
                "yes_no",
                "nan",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "yes_no_dk_ref",
                "nan",
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
                "nan",
                "consent_gatekeeper",
                "nan",
                "nan",
                "consent_basic_questions",
                "end_of_interview_1",
                "NR1",
                "NR2",
                "NR3end",
                "consent_gatekeeper_2",
                "end_of_interview_3",
                "gatekeeper",
                "nan",
                "selection",
                "consent_respondent",
                "end_of_interview_2",
                "age_verification",
                "selection",
            ],
            type_survey=[
                "yes_no",
                "username",
                "text",
                "select_one region",
                "select_one constituency",
                "text",
                "begin_group",
                "nan",
                "select_one consent",
                "nan",
                "nan",
                "select_one yes_no",
                "note",
                "integer",
                "integer",
                "note",
                "select_one yes_no",
                "note",
                "end_group",
                "nan",
                "begin_group",
                "select_one consent",
                "note",
                "select_one yes_no",
                "end_group",
            ],
        )
    ]

from typing import Any

import pytest

from mex.common.models import (
    ExtractedActivity,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.testing import Joker
from mex.common.types import (
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.odk.model import ODKData
from mex.extractors.odk.transform import (
    get_variable_groups_from_raw_data,
    transform_odk_data_to_extracted_variables,
    transform_odk_resources_to_mex_resources,
    transform_odk_variable_groups_to_extracted_variable_groups,
)


@pytest.fixture
def extracted_international_projects_activities() -> list[ExtractedActivity]:
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


def test_transform_odk_resources_to_mex_resources(
    odk_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    external_partner_and_publisher_by_label: dict[str, MergedOrganizationIdentifier],
    extracted_international_projects_activities: list[ExtractedActivity],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    international_project_stable_target_id = next(
        filter(
            lambda x: x.identifierInPrimarySource == "0000-1000",
            extracted_international_projects_activities,
        )
    ).stableTargetId

    resources, is_part_of = transform_odk_resources_to_mex_resources(
        odk_resource_mappings,
        unit_stable_target_ids_by_synonym,
        external_partner_and_publisher_by_label,
        extracted_international_projects_activities,
        extracted_primary_sources["mex"],
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": "00000000000000",
        "hasLegalBasis": [
            {
                "language": TextLanguage.EN,
                "value": "Informed consent",
            },
        ],
        "identifierInPrimarySource": "test_raw_data",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [
            {"value": "dolor", "language": TextLanguage.EN},
            {"value": "sit", "language": TextLanguage.DE},
        ],
        "contact": [str(unit_stable_target_ids_by_synonym["C1"])],
        "contributingUnit": [str(unit_stable_target_ids_by_synonym["C1"])],
        "description": [{"value": "amet", "language": TextLanguage.EN}],
        "externalPartner": [
            str(external_partner_and_publisher_by_label["consetetur"]),
            str(external_partner_and_publisher_by_label["invidunt"]),
            "gnhGhU3ATowGuV0KJwGuxB",
        ],
        "keyword": [
            {"value": "elitr"},
            {"value": "sed", "language": TextLanguage.EN},
            {"value": "diam", "language": TextLanguage.EN},
        ],
        "language": ["https://mex.rki.de/item/language-2"],
        "meshId": ["http://id.nlm.nih.gov/mesh/D000086382"],
        "method": [
            {"value": "nonumy", "language": TextLanguage.EN},
            {"value": "eirmod", "language": TextLanguage.DE},
        ],
        "methodDescription": [{"value": "tempor", "language": TextLanguage.EN}],
        "publisher": [
            str(external_partner_and_publisher_by_label["invidunt"]),
            str(external_partner_and_publisher_by_label["consetetur"]),
        ],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-2",
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-15"],
        "resourceTypeSpecific": [
            {
                "language": TextLanguage.EN,
                "value": "tempor",
            },
        ],
        "rights": [{"value": "ut labore", "language": TextLanguage.DE}],
        "sizeOfDataBasis": "et dolore",
        "spatial": [
            {"value": "magna", "language": TextLanguage.DE},
            {"value": "magna", "language": TextLanguage.EN},
        ],
        "temporal": "2021-07-27 - 2021-12-31",
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-37",
        ],
        "title": [
            {"value": "aliquyam", "language": TextLanguage.EN},
            {"value": "erat", "language": TextLanguage.DE},
        ],
        "unitInCharge": [str(unit_stable_target_ids_by_synonym["C1"])],
        "wasGeneratedBy": str(international_project_stable_target_id),
    }
    assert resources["test_raw_data"].model_dump(exclude_defaults=True) == expected
    assert is_part_of == []
    resources_organizations_empty_or_created, _ = (
        transform_odk_resources_to_mex_resources(
            odk_resource_mappings,
            unit_stable_target_ids_by_synonym,
            {},
            extracted_international_projects_activities,
            extracted_primary_sources["mex"],
        )
    )
    assert (
        resources_organizations_empty_or_created["test_raw_data"].model_dump()[
            "publisher"
        ]
        == []
    )
    assert resources_organizations_empty_or_created["test_raw_data"].model_dump()[
        "externalPartner"
    ] == [
        "dHOP0smFLofntMGAA4Z89M",
        "hmAkmTAONYQFjxpqrElERm",
        "gnhGhU3ATowGuV0KJwGuxB",
    ]


def test_get_variable_groups_from_raw_data(
    odk_raw_data: list[ODKData],
) -> None:
    groups = get_variable_groups_from_raw_data(odk_raw_data)

    expected = {
        "gatekeeper": [
            {
                "type": "begin_group",
                "name": "gatekeeper",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Introduction of study to gatekeeper",
                "label::Otjiherero (hz)": "Introduction of study to gatekeeper",
            },
            {
                "type": "select_one consent",
                "name": "consent_gatekeeper",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "**Verbal consent**",
                "label::Otjiherero (hz)": "**Omaitaverero wokotjinyo**",
            },
            {
                "type": "select_one yes_no",
                "name": "consent_basic_questions",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Are you",
                "label::Otjiherero (hz)": "Ove moyenene okunyamukura omapuriro inga?",
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
            },
        ],
        "selection": [
            {
                "type": "begin_group",
                "name": "selection",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Selection of respondent",
                "label::Otjiherero (hz)": "Selection of respondent",
            },
            {
                "type": "select_one consent",
                "name": "consent_respondent",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "**Verbal consent**",
                "label::Otjiherero (hz)": "**Omaitaverero wokotjinyo**",
            },
            {
                "type": "select_one yes_no",
                "name": "age_verification",
                "file_name": "test_raw_data.xlsx",
                "label::English (en)": "Are you currently 18 years old or older?",
                "label::Otjiherero (hz)": "Una ozombura 18 nokombanda?",
            },
        ],
    }
    assert groups == expected


def test_transform_odk_variable_groups_to_extracted_variable_groups(
    odk_variable_groups: dict[str, list[dict[str, str | float]]],
    extracted_resources_odk: list[ExtractedResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_variable_groups = (
        transform_odk_variable_groups_to_extracted_variable_groups(
            odk_variable_groups,
            extracted_resources_odk,
            extracted_primary_sources["odk"],
        )
    )

    expected = {
        "identifier": Joker(),
        "stableTargetId": Joker(),
        "hadPrimarySource": extracted_primary_sources["odk"].stableTargetId,
        "identifierInPrimarySource": "begin_group-gatekeeper",
        "containedBy": [extracted_resources_odk[0].stableTargetId],
        "label": [{"value": "gatekeeper"}],
    }
    assert extracted_variable_groups[0].model_dump(exclude_defaults=True) == expected


def test_transform_odk_data_to_extracted_variables(
    extracted_resources_odk: list[ExtractedResource],
    extracted_variable_groups_odk: list[ExtractedVariableGroup],
    odk_variable_groups: dict[str, list[dict[str, str | float]]],
    odk_raw_data: list[ODKData],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_variables = transform_odk_data_to_extracted_variables(
        extracted_resources_odk,
        extracted_variable_groups_odk,
        odk_variable_groups,
        odk_raw_data,
        extracted_primary_sources["odk"],
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": str(extracted_primary_sources["odk"].stableTargetId),
        "identifierInPrimarySource": "gatekeeper",
        "stableTargetId": Joker(),
        "belongsTo": [str(extracted_variable_groups_odk[0].stableTargetId)],
        "dataType": "begin_group",
        "label": [
            {
                "value": "Introduction of study to gatekeeper",
                "language": TextLanguage.EN,
            },
            {
                "value": "Introduction of study to gatekeeper",
                "language": TextLanguage.EN,
            },
        ],
        "usedIn": [str(extracted_resources_odk[0].stableTargetId)],
    }
    assert extracted_variables[0].model_dump(exclude_defaults=True) == expected
    expected = {
        "hadPrimarySource": str(extracted_primary_sources["odk"].stableTargetId),
        "identifierInPrimarySource": "consent_gatekeeper",
        "belongsTo": [str(extracted_variable_groups_odk[0].stableTargetId)],
        "dataType": "select_one consent",
        "label": [
            {"value": "**Verbal consent**"},
            {"value": "**Omaitaverero wokotjinyo**"},
        ],
        "usedIn": [str(extracted_resources_odk[0].stableTargetId)],
        "valueSet": [
            "consent",
            "I AGREE with the above statements and wish to take part in the survey",
            "Ami ME ITAVERE komaheya nge ri kombanda mba nu otji me raisa kutja mbi nonḓero okukara norupa mongonḓononeno.",
            "I do NOT AGREE to take part in the survey",
            "Ami HI NOKUITAVERA okukara norupa mongonḓononeno.",
        ],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert extracted_variables[1].model_dump(exclude_defaults=True) == expected

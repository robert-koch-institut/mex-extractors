from typing import Any

import pytest

from mex.common.identity import get_provider
from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
)
from mex.odk.model import ODKData
from mex.odk.transform import (
    get_external_partner_and_publisher_by_label,
    get_variable_groups_from_raw_data,
    transform_odk_data_to_extracted_variables,
    transform_odk_resources_to_mex_resources,
    transform_odk_variable_groups_to_extracted_variable_groups,
)


def test_transform_odk_resources_to_mex_resources(
    odk_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    external_partner_and_publisher_by_label: dict[str, ExtractedOrganization],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    identity_provider = get_provider()
    identity = identity_provider.assign(
        extracted_primary_sources["international-projects"].stableTargetId,
        "testAAbr",
    )  # "testAAbr" is the default value from test mapping

    resources = transform_odk_resources_to_mex_resources(
        odk_resource_mappings,
        unit_stable_target_ids_by_synonym,
        extracted_primary_sources["international-projects"],
        external_partner_and_publisher_by_label,
        extracted_primary_sources["mex"],
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": "00000000000000",
        "identifierInPrimarySource": "test_raw_data",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [
            {"value": "dolor", "language": "en"},
            {"value": "sit", "language": "de"},
        ],
        "contact": [unit_stable_target_ids_by_synonym["C1"]],
        "contributingUnit": [unit_stable_target_ids_by_synonym["C1"]],
        "description": [{"value": "amet", "language": "en"}],
        "externalPartner": [
            external_partner_and_publisher_by_label["consetetur"],
            external_partner_and_publisher_by_label["invidunt"],
        ],
        "keyword": [
            {"value": "elitr"},
            {"value": "sed", "language": "en"},
            {"value": "diam", "language": "en"},
        ],
        "language": ["https://mex.rki.de/item/language-2"],
        "meshId": ["http://id.nlm.nih.gov/mesh/D000086382"],
        "method": [
            {"value": "nonumy", "language": "en"},
            {"value": "eirmod", "language": "de"},
        ],
        "methodDescription": [{"value": "tempor", "language": "en"}],
        "publisher": [
            external_partner_and_publisher_by_label["invidunt"],
            external_partner_and_publisher_by_label["consetetur"],
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-8"],
        "rights": [{"value": "ut labore", "language": "de"}],
        "sizeOfDataBasis": "et dolore",
        "spatial": [
            {"value": "magna", "language": "de"},
            {"value": "magna", "language": "en"},
        ],
        "temporal": "2021-07-27 - 2021-12-31",
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-35",
        ],
        "title": [
            {"value": "aliquyam", "language": "en"},
            {"value": "erat", "language": "de"},
        ],
        "unitInCharge": [unit_stable_target_ids_by_synonym["C1"]],
        "wasGeneratedBy": identity.stableTargetId,
    }
    assert resources[0].model_dump(exclude_defaults=True) == expected

    resources_without_organizations = transform_odk_resources_to_mex_resources(
        odk_resource_mappings,
        unit_stable_target_ids_by_synonym,
        extracted_primary_sources["international-projects"],
        {},
        extracted_primary_sources["mex"],
    )
    assert resources_without_organizations[0].model_dump()["publisher"] == []
    assert resources_without_organizations[0].model_dump()["externalPartner"] == []


@pytest.mark.usefixtures(
    "mocked_wikidata",
)
def test_get_external_partner_and_publisher_by_label(
    odk_resource_mappings: list[dict[str, Any]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    organization_dict = get_external_partner_and_publisher_by_label(
        odk_resource_mappings, extracted_primary_sources["wikidata"]
    )

    assert organization_dict == {
        "invidunt": Joker(),
        "consetetur": Joker(),
        " sadipscing  ": Joker(),
    }


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
        "hadPrimarySource": extracted_primary_sources["odk"].stableTargetId,
        "identifierInPrimarySource": "gatekeeper",
        "stableTargetId": Joker(),
        "belongsTo": [extracted_variable_groups_odk[0].stableTargetId],
        "label": [
            {"value": "Introduction of study to gatekeeper", "language": "en"},
            {"value": "Introduction of study to gatekeeper", "language": "en"},
        ],
        "usedIn": [extracted_resources_odk[0].stableTargetId],
    }
    assert extracted_variables[0].model_dump(exclude_defaults=True) == expected
    expected = {
        "hadPrimarySource": extracted_primary_sources["odk"].stableTargetId,
        "identifierInPrimarySource": "consent_gatekeeper",
        "belongsTo": [extracted_variable_groups_odk[0].stableTargetId],
        "label": [
            {"value": "**Verbal consent**"},
            {"value": "**Omaitaverero wokotjinyo**"},
        ],
        "usedIn": [extracted_resources_odk[0].stableTargetId],
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

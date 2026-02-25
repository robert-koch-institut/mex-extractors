from typing import TYPE_CHECKING

import pytest

from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.odk.transform import (
    transform_odk_data_to_extracted_variables,
    transform_odk_resources_to_mex_resources,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        ExtractedActivity,
        ExtractedResource,
        ResourceMapping,
        VariableMapping,
    )
    from mex.extractors.odk.model import ODKData


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_odk_resources_to_mex_resources(
    odk_resource_mappings: list[ResourceMapping],
    odk_merged_organization_ids_by_str: dict[str, MergedOrganizationIdentifier],
    international_projects_extracted_activities: list[ExtractedActivity],
) -> None:
    international_project_stable_target_id = next(
        filter(
            lambda x: x.identifierInPrimarySource == "0000-1000",
            international_projects_extracted_activities,
        )
    ).stableTargetId
    resources = transform_odk_resources_to_mex_resources(
        odk_resource_mappings,
        odk_merged_organization_ids_by_str,
        international_projects_extracted_activities,
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": get_extracted_primary_source_id_by_name("odk"),
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
        "contact": get_unit_merged_id_by_synonym("C1"),
        "contributingUnit": get_unit_merged_id_by_synonym("C1"),
        "description": [{"value": "amet", "language": TextLanguage.EN}],
        "externalPartner": [
            str(odk_merged_organization_ids_by_str["consetetur"]),
            str(odk_merged_organization_ids_by_str["invidunt"]),
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
            str(odk_merged_organization_ids_by_str["invidunt"]),
            str(odk_merged_organization_ids_by_str["consetetur"]),
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
        "unitInCharge": get_unit_merged_id_by_synonym("C1"),
        "wasGeneratedBy": str(international_project_stable_target_id),
    }
    assert resources[0][0].model_dump(exclude_defaults=True) == expected
    resources_organizations_empty_or_created = transform_odk_resources_to_mex_resources(
        odk_resource_mappings,
        {},
        international_projects_extracted_activities,
    )
    assert (
        resources_organizations_empty_or_created[0][0].model_dump()["publisher"] == []
    )
    assert resources_organizations_empty_or_created[0][0].model_dump()[
        "externalPartner"
    ] == [
        "dHOP0smFLofntMGAA4Z89M",
        "hmAkmTAONYQFjxpqrElERm",
        "gnhGhU3ATowGuV0KJwGuxB",
    ]


def test_transform_odk_data_to_extracted_variables(
    odk_extracted_resources: list[ExtractedResource],
    odk_raw_data: list[ODKData],
    odk_variable_mapping: VariableMapping,
) -> None:
    extracted_variables = transform_odk_data_to_extracted_variables(
        odk_extracted_resources,
        odk_raw_data,
        odk_variable_mapping,
    )
    expected = {
        "hadPrimarySource": "cdIHcORh7ClqN0YCaCPQ8q",
        "identifierInPrimarySource": "start_test_raw_data",
        "dataType": "yes_no",
        "label": [{"value": "start"}],
        "usedIn": ["fYOpRIonO2AedXxBxX9ZBk"],
        "valueSet": ["yes_no, Yes", "yes_no", "yes_no, No", "yes_no"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert extracted_variables[0].model_dump(exclude_defaults=True) == expected
    expected = {
        "hadPrimarySource": "cdIHcORh7ClqN0YCaCPQ8q",
        "identifierInPrimarySource": "username_test_raw_data",
        "dataType": "username",
        "label": [{"value": "username", "language": "en"}],
        "usedIn": ["fYOpRIonO2AedXxBxX9ZBk"],
        "description": [
            {"value": "Store username of interviewer.", "language": "en"},
            {"value": "Store username of interviewer.", "language": "en"},
        ],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert extracted_variables[1].model_dump(exclude_defaults=True) == expected

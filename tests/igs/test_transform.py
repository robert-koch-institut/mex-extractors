from typing import TYPE_CHECKING

import pytest

from mex.common.types import (
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
    TextLanguage,
)
from mex.extractors.igs.transform import (
    transform_igs_access_platform,
    transform_igs_extracted_resource,
    transform_igs_schemas_to_variables,
    transformed_igs_schemas_to_variable_group,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        AccessPlatformMapping,
        ExtractedAccessPlatform,
        ExtractedContactPoint,
        ExtractedOrganization,
        ExtractedOrganizationalUnit,
        ResourceMapping,
        VariableMapping,
    )
    from mex.extractors.igs.model import IGSInfo, IGSSchema


@pytest.mark.usefixtures("mocked_igs", "mocked_wikidata")
def test_transform_igs_extracted_resource(  # noqa: PLR0913, PLR0917
    igs_resource_mapping: ResourceMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
    igs_schemas: dict[str, IGSSchema],
    igs_info: IGSInfo,
    igs_endpoint_counts: dict[str, str],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    extracted_resource = transform_igs_extracted_resource(
        igs_resource_mapping,
        igs_extracted_contact_points_by_mail_str,
        extracted_access_platform,
        extracted_organization_rki,
        igs_schemas,
        igs_info,
        igs_endpoint_counts,
        {"PATHOGEN": [MergedResourceIdentifier.generate(seed=342)]},
    )
    assert extracted_resource["PATHOGEN"].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("igs"),
        "identifierInPrimarySource": "test_title_PATHOGEN",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "start": ["1970"],
        "sizeOfDataBasis": "Anzahl Proben: 7",
        "contact": [
            igs_extracted_contact_points_by_mail_str["contactc@rki.de"].stableTargetId
        ],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Pathogen", "language": TextLanguage.DE}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "accessPlatform": [extracted_access_platform.stableTargetId],
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "keyword": [
            {"value": "test keyword", "language": TextLanguage.DE},
            {"value": "Pathogen", "language": TextLanguage.DE},
            {"value": "PATHOGEN"},
        ],
        "healthCategory": ["https://mex.rki.de/item/health-category-1"],
        "publisher": [extracted_organization_rki.stableTargetId],
        "qualityInformation": [
            {"value": "Test Präfix", "language": TextLanguage.DE},
            {"value": "Anzahl tests: 42", "language": TextLanguage.DE},
        ],
        "identifier": "eV8CKqNqhgnJ5UUtMCziDi",
        "relatedResource": ["bFQoRhcVH5DHZW"],
        "stableTargetId": "vqycPRN9Z9KC97eLt9oAP",
    }


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_igs_access_platform(
    igs_access_platform_mapping: AccessPlatformMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    extracted_access_platform = transform_igs_access_platform(
        igs_access_platform_mapping,
        igs_extracted_contact_points_by_mail_str,
    )

    assert extracted_access_platform.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("igs"),
        "identifierInPrimarySource": "https://igs",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "endpointDescription": {
            "language": TextLanguage.EN,
            "title": "test title",
            "url": "https://rki.de:4200/api",
        },
        "endpointType": "https://mex.rki.de/item/api-type-1",
        "endpointURL": {"url": "https://rki.de:4100"},
        "contact": [
            igs_extracted_contact_points_by_mail_str["contactc@rki.de"].stableTargetId
        ],
        "description": [{"value": "test description", "language": TextLanguage.EN}],
        "landingPage": [{"url": "https://rki.de:4100/docs"}],
        "title": [{"language": TextLanguage.EN, "value": "IGS Open API"}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "identifier": "ciq6oOPVAqov2QX9KQcsFd",
        "stableTargetId": "g72piPlFnLPbe8dkRoCBsx",
    }


@pytest.mark.usefixtures("mocked_igs")
def test_transformed_igs_schemas_to_variable_group(
    filtered_igs_schemas: dict[str, IGSSchema],
) -> None:
    extracted_variable_groups = transformed_igs_schemas_to_variable_group(
        filtered_igs_schemas, [MergedResourceIdentifier.generate(seed=42)]
    )
    expected = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("igs"),
        "identifierInPrimarySource": "variable-group-SchemaCreation",
        "containedBy": ["bFQoRhcVH5DHU6"],
        "label": [{"value": "SchemaCreation", "language": TextLanguage.EN}],
        "identifier": "uVz9KCEmkU9Nj3IM5Rphv",
        "stableTargetId": "cpwz2hs0xnNLRJYlngToaG",
    }

    assert extracted_variable_groups[0].model_dump(exclude_defaults=True) == expected


def test_transform_igs_schemas_to_variables(
    filtered_igs_schemas: dict[str, IGSSchema],
    igs_variable_mapping: VariableMapping,
) -> None:
    extracted_variables = transform_igs_schemas_to_variables(
        filtered_igs_schemas,
        [MergedResourceIdentifier.generate(seed=42)],
        {
            "variable-group-SchemaCreation": MergedVariableGroupIdentifier.generate(
                seed=43
            )
        },
        igs_variable_mapping,
    )
    expected = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("igs"),
        "identifierInPrimarySource": "variable-group-SchemaCreation_schemas",
        "dataType": "date",
        "description": [{"value": "test_description"}],
        "label": [{"value": "schemas", "language": TextLanguage.DE}],
        "usedIn": ["bFQoRhcVH5DHU6"],
        "belongsTo": ["bFQoRhcVH5DHU7"],
        "identifier": "cnZF2Q4cKgTkXrItCRthOY",
        "stableTargetId": "bxrMKpfWgjAvJ5a0B2WacI",
    }
    assert extracted_variables[0].model_dump(exclude_defaults=True) == expected

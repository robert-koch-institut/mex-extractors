import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedOrganization,
    ResourceMapping,
    VariableMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
)
from mex.extractors.igs.model import IGSInfo, IGSSchema
from mex.extractors.igs.transform import (
    transform_igs_access_platform,
    transform_igs_extracted_resource,
    transform_igs_schemas_to_variables,
    transformed_igs_schemas_to_variable_group,
)


@pytest.mark.usefixtures("mocked_igs", "mocked_wikidata")
def test_transform_igs_extracted_resource(  # noqa: PLR0913
    igs_resource_mapping: ResourceMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
    igs_schemas: dict[str, IGSSchema],
    igs_info: IGSInfo,
    igs_endpoint_counts: dict[str, str],
) -> None:
    extracted_resource = transform_igs_extracted_resource(
        igs_resource_mapping,
        igs_extracted_contact_points_by_mail_str,
        extracted_access_platform,
        extracted_organization_rki,
        igs_schemas,
        igs_info,
        igs_endpoint_counts,
    )
    assert extracted_resource["PATHOGEN"].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "test_title_PATHOGEN",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "created": "1970",
        "sizeOfDataBasis": "5",
        "contact": ["g0ZXxKhXuUiSqdpAdhuKlb"],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Pathogen", "language": "de"}],
        "unitInCharge": ["6rqNvZSApUHlz8GkkVP48"],
        "accessPlatform": ["g72piPlFnLPbe8dkRoCBsx"],
        "contributingUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "keyword": [
            {"value": "test keyword", "language": "de"},
            {"value": "PATHOGEN"},
            {"value": "Pathogen", "language": "de"},
        ],
        "publisher": ["fxIeF3TWocUZoMGmBftJ6x"],
        "qualityInformation": [
            {"value": "Anzahl tests: 42", "language": "de"},
            {"value": "Anzahl Genomsequenzen: 7", "language": "de"},
        ],
        "identifier": "eV8CKqNqhgnJ5UUtMCziDi",
        "stableTargetId": "vqycPRN9Z9KC97eLt9oAP",
    }


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_igs_access_platform(
    igs_access_platform_mapping: AccessPlatformMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
) -> None:
    extracted_access_platform = transform_igs_access_platform(
        igs_access_platform_mapping,
        igs_extracted_contact_points_by_mail_str,
    )

    assert extracted_access_platform.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "https://igs",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "endpointDescription": {
            "language": "en",
            "title": "test title",
            "url": "https://rki.de:4200/api",
        },
        "endpointType": "https://mex.rki.de/item/api-type-1",
        "endpointURL": {"url": "https://rki.de:4100"},
        "contact": ["g0ZXxKhXuUiSqdpAdhuKlb"],
        "description": [{"value": "test description", "language": "en"}],
        "landingPage": [{"url": "https://rki.de:4100/docs"}],
        "title": [{"language": "en", "value": "IGS Open API"}],
        "unitInCharge": ["6rqNvZSApUHlz8GkkVP48"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


@pytest.mark.usefixtures("mocked_igs")
def test_transformed_igs_schemas_to_variable_group(
    igs_schemas: dict[str, IGSSchema],
) -> None:
    extracted_variable_groups = transformed_igs_schemas_to_variable_group(
        igs_schemas, MergedResourceIdentifier.generate(seed=42)
    )
    expected = {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "igsmodels__enums__Pathogen",
        "containedBy": ["bFQoRhcVH5DHU6"],
        "label": [{"value": "igsmodels__enums__Pathogen", "language": "en"}],
        "identifier": "bmzsPxrn1mqZm8GCmKaJ5I",
        "stableTargetId": "qGSnMxJYiNTJeUntnV3Vy",
    }

    assert extracted_variable_groups[0].model_dump(exclude_defaults=True) == expected


def test_transform_igs_schemas_to_variables(
    igs_schemas: dict[str, IGSSchema],
    igs_variable_mapping: VariableMapping,
    igs_variable_pathogen_mapping: VariableMapping,
) -> None:
    extracted_variables = transform_igs_schemas_to_variables(
        igs_schemas,
        MergedResourceIdentifier.generate(seed=42),
        {"igsmodels__enums__Pathogen": MergedVariableGroupIdentifier.generate(seed=43)},
        igs_variable_mapping,
        igs_variable_pathogen_mapping,
    )
    expected = {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "pathogen_PATHOGEN",
        "dataType": "string",
        "label": [{"value": "PATHOGEN"}],
        "usedIn": ["bFQoRhcVH5DHU6"],
        "belongsTo": ["bFQoRhcVH5DHU7"],
        "description": [{"value": "Pathogen", "language": "de"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert extracted_variables[0].model_dump(exclude_defaults=True) == expected

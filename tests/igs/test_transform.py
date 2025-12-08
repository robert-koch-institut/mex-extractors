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
from mex.extractors.igs.model import IGSSchema
from mex.extractors.igs.transform import (
    get_enums_by_property_name,
    transform_igs_access_platform,
    transform_igs_extracted_resource,
    transform_igs_schemas_to_variables,
    transformed_igs_schemas_to_variable_group,
)


@pytest.mark.usefixtures("mocked_igs")
def test_transform_igs_extracted_resource(
    igs_resource_mapping: ResourceMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    extracted_resource = transform_igs_extracted_resource(
        igs_resource_mapping,
        igs_extracted_contact_points_by_mail_str,
        extracted_access_platform,
        extracted_organization_rki,
    )
    assert extracted_resource.model_dump(exclude_defaults=True) == {
        "accessPlatform": [
            "g72piPlFnLPbe8dkRoCBsx",
        ],
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "test_id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "contact": ["g0ZXxKhXuUiSqdpAdhuKlb"],
        "publisher": [
            "fxIeF3TWocUZoMGmBftJ6x",
        ],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Pathogen", "language": "de"}],
        "unitInCharge": ["6rqNvZSApUHlz8GkkVP48"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
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
        "contact": ["cGyT8sVLtQTF7vK24LoOk6"],
        "description": [{"value": "test description", "language": "en"}],
        "landingPage": [{"url": "https://rki.de:4100/docs"}],
        "title": [{"language": "en", "value": "IGS Open API"}],
        "unitInCharge": ["6rqNvZSApUHlz8GkkVP48"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_transformed_igs_schemas_to_variable_group(
    igs_schemas: dict[str, IGSSchema],
) -> None:
    extracted_variable_groups = transformed_igs_schemas_to_variable_group(
        igs_schemas, MergedResourceIdentifier.generate(seed=42)
    )
    expected = {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "Pathogen",
        "containedBy": ["bFQoRhcVH5DHU6"],
        "label": [{"value": "Pathogen", "language": "en"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

    assert extracted_variable_groups[0].model_dump(exclude_defaults=True) == expected


def test_get_enums_by_property_name(
    igs_schemas: dict[str, IGSSchema],
) -> None:
    enums_by_property_name = get_enums_by_property_name(igs_schemas)
    assert enums_by_property_name == {"schemas": ["PATHOGEN"]}


def test_transform_igs_schemas_to_variables(
    igs_schemas: dict[str, IGSSchema],
    igs_variable_mapping: VariableMapping,
    igs_variable_pathogen_mapping: VariableMapping,
) -> None:
    extracted_variables = transform_igs_schemas_to_variables(
        igs_schemas,
        MergedResourceIdentifier.generate(seed=42),
        {"Pathogen": MergedVariableGroupIdentifier.generate(seed=43)},
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

import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedContactPoint,
    ExtractedPrimarySource,
    ResourceMapping,
    VariableMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
)
from mex.extractors.igs.model import IGSInfo, IGSSchema
from mex.extractors.igs.transform import (
    get_enums_by_property_name,
    transform_igs_access_platform,
    transform_igs_schemas_to_resources,
    transform_igs_schemas_to_variables,
    transformed_igs_schemas_to_variable_group,
)


@pytest.mark.usefixtures("mocked_igs")
def test_transform_igs_schemas_to_resources(
    igs_schemas: dict[str, IGSSchema],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    igs_resource_mapping: ResourceMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_resources = transform_igs_schemas_to_resources(
        igs_schemas,
        IGSInfo(title="title", version="-1"),
        extracted_primary_sources["igs"],
        igs_resource_mapping,
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )
    assert extracted_resources[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "IGS_title_v-1",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "contact": ["g0ZXxKhXuUiSqdpAdhuKlb"],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Pathogen", "language": "de"}],
        "unitInCharge": ["bFQoRhcVH5DHU8"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_transform_igs_access_platform(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    igs_access_platform_mapping: AccessPlatformMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_access_platform = transform_igs_access_platform(
        extracted_primary_sources["igs"],
        igs_access_platform_mapping,
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )

    assert extracted_access_platform.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "https://igs",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "contact": ["cGyT8sVLtQTF7vK24LoOk6"],
        "unitInCharge": ["bFQoRhcVH5DHU8"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_transformed_igs_schemas_to_variable_group(
    igs_schemas: dict[str, IGSSchema],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    igs_info: IGSInfo,
) -> None:
    extracted_variable_groups = transformed_igs_schemas_to_variable_group(
        igs_schemas,
        {"IGS_test_title_vtest_version": MergedResourceIdentifier.generate(seed=42)},
        extracted_primary_sources["igs"],
        igs_info,
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
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    igs_variable_mapping: VariableMapping,
    igs_variable_pathogen_mapping: VariableMapping,
    igs_info: IGSInfo,
) -> None:
    extracted_variables = transform_igs_schemas_to_variables(
        igs_schemas,
        {"IGS_test_title_vtest_version": MergedResourceIdentifier.generate(seed=42)},
        extracted_primary_sources["igs"],
        {"Pathogen": MergedVariableGroupIdentifier.generate(seed=43)},
        igs_variable_mapping,
        igs_variable_pathogen_mapping,
        igs_info,
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

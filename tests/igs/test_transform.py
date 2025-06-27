import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedContactPoint,
    ExtractedPrimarySource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.igs.model import IGSSchema
from mex.extractors.igs.transform import (
    transform_igs_access_platform,
    transform_igs_schemas_to_resources,
)


@pytest.mark.usefixtures("mocked_igs")
def test_transfom_igs_schemas_to_resources(
    igs_schemas: dict[str, IGSSchema],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    igs_resource_mapping: ResourceMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_resources = transform_igs_schemas_to_resources(
        igs_schemas,
        extracted_primary_sources["igs"],
        igs_resource_mapping,
        extracted_igs_contact_points_by_mail,
        unit_stable_target_ids_by_synonym,
    )
    assert extracted_resources["PATHOGEN"].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
        "identifierInPrimarySource": "IGS_PATHOGEN",
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

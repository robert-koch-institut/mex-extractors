import pytest

from mex.common.models import (
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    TextLanguage,
)
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.open_data.transform import (
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_resource_version_to_mex_resource,
)


@pytest.mark.usefixtures("mocked_ldap", "mocked_open_data")
def test_transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    mocked_parent_resource_reponse: OpenDataParentResource,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_persons: list[ExtractedPerson],
    mocked_open_data_parent_resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_contact_point: ExtractedContactPoint,
) -> None:
    unit_stable_target_ids_by_synonym = {
        "mf4": Identifier.generate(seed=999),
        "XY": Identifier.generate(seed=959),
    }

    mex_sources = list(
        transform_open_data_parent_resource_to_mex_resource(
            mocked_parent_resource_reponse,
            extracted_primary_sources["open-data"],
            mocked_open_data_persons,
            unit_stable_target_ids_by_synonym,
            mocked_open_data_parent_resource_mapping,
            extracted_organization_rki,
            mocked_contact_point,
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": str(extracted_primary_sources["open-data"].stableTargetId),
        "identifierInPrimarySource": "Eins",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "created": "2021",
        "hasPersonalData": "https://mex.rki.de/item/personal-data-2",
        "license": "https://mex.rki.de/item/license-1",
        "contact": [str(mocked_contact_point[0].stableTargetId)],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"value": "Dumdidumdidum"}],
        "unitInCharge": [str(Identifier.generate(seed=999))],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-1"
        ],
        "contributor": [str(mocked_open_data_persons[0].stableTargetId)],
        "description": [
            {"language": TextLanguage.EN, "value": "Test1 <a href='test/2'>test3</a>"}
        ],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-14"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_open_data")
def test_transform_open_data_resource_version_to_mex_resource(  # noqa: PLR0913
    mocked_resource_version: OpenDataResourceVersion,
    mocked_extracted_parent_resource: list[ExtractedResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_persons: list[ExtractedPerson],
    mocked_open_data_distribution: list[ExtractedDistribution],
    mocked_open_data_resource_version_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_contact_point: ExtractedContactPoint,
) -> None:
    unit_stable_target_ids_by_synonym = {
        "mf4": Identifier.generate(seed=999),
        "XY": Identifier.generate(seed=959),
    }

    mex_sources = list(
        transform_open_data_resource_version_to_mex_resource(
            mocked_resource_version,
            extracted_primary_sources["open-data"],
            mocked_open_data_persons,
            mocked_extracted_parent_resource,
            unit_stable_target_ids_by_synonym,
            mocked_open_data_distribution,
            mocked_open_data_resource_version_mapping,
            extracted_organization_rki,
            mocked_contact_point,
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": str(extracted_primary_sources["open-data"].stableTargetId),
        "identifierInPrimarySource": "1001",
        "isPartOf": [mocked_extracted_parent_resource[0].stableTargetId],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "created": "2021-01-01T01:01:01Z",
        "hasPersonalData": "https://mex.rki.de/item/personal-data-2",
        "license": "https://mex.rki.de/item/license-1",
        "contact": [str(mocked_contact_point[0].stableTargetId)],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"value": "Dumdidumdidum"}],
        "unitInCharge": [str(Identifier.generate(seed=999))],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-1"
        ],
        "contributor": [str(mocked_open_data_persons[0].stableTargetId)],
        "documentation": [{"url": "should be transformed"}],
        "distribution": [str(mocked_open_data_distribution[0].stableTargetId)],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-14"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

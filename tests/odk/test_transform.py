from mex.common.models import ExtractedPrimarySource, ExtractedResource
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.odk.model import ODKData
from mex.extractors.odk.transform import (
    transform_odk_data_to_extracted_variables,
    transform_odk_resources_to_mex_resources,
)


def test_transform_odk_resources_to_mex_resources(
    odk_resource_mappings: list[AnyMappingModel],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    external_partner_and_publisher_by_label: dict[str, MergedOrganizationIdentifier],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    resources, is_part_of = transform_odk_resources_to_mex_resources(
        odk_resource_mappings,
        unit_stable_target_ids_by_synonym,
        external_partner_and_publisher_by_label,
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
    }
    assert resources["test_raw_data"].model_dump(exclude_defaults=True) == expected
    assert is_part_of == []
    resources_organizations_empty_or_created, _ = (
        transform_odk_resources_to_mex_resources(
            odk_resource_mappings,
            unit_stable_target_ids_by_synonym,
            {},
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


def test_transform_odk_data_to_extracted_variables(
    extracted_resources_odk: list[ExtractedResource],
    odk_raw_data: list[ODKData],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_variables = transform_odk_data_to_extracted_variables(
        extracted_resources_odk,
        odk_raw_data,
        extracted_primary_sources["odk"],
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": str(extracted_primary_sources["odk"].stableTargetId),
        "identifierInPrimarySource": "start",
        "stableTargetId": Joker(),
        "dataType": "start",
        "label": [{"value": "start"}],
        "usedIn": [str(extracted_resources_odk[0].stableTargetId)],
        "description": [{"value": "nan"}, {"value": "nan"}],
        "valueSet": ["start"],
    }
    assert extracted_variables[0].model_dump(exclude_defaults=True) == expected
    expected = {
        "hadPrimarySource": str(extracted_primary_sources["odk"].stableTargetId),
        "identifierInPrimarySource": "username",
        "dataType": "username",
        "label": [{"value": "username", "language": TextLanguage.EN}],
        "usedIn": [str(extracted_resources_odk[0].stableTargetId)],
        "description": [
            {"value": "Store username of interviewer.", "language": TextLanguage.EN},
            {"value": "Store username of interviewer.", "language": TextLanguage.EN},
        ],
        "valueSet": ["username"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert extracted_variables[1].model_dump(exclude_defaults=True) == expected

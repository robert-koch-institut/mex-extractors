from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_datatype import MetaDataType
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_type import MetaType
from mex.extractors.ifsg.transform import (
    get_instrument_tool_or_apparatus,
    transform_ifsg_data_to_mex_variable_group,
    transform_ifsg_data_to_mex_variables,
    transform_resource_disease_to_mex_resource,
    transform_resource_parent_to_mex_resource,
    transform_resource_state_to_mex_resource,
)


def test_transform_resource_parent_to_mex_resource(
    resource_parent: ResourceMapping,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_stable_target_ids: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_resource = transform_resource_parent_to_mex_resource(
        resource_parent, extracted_primary_sources["ifsg"], unit_stable_target_ids
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": str(extracted_primary_sources["ifsg"].stableTargetId),
        "identifierInPrimarySource": "ifsg-parent",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "alternativeTitle": [{"value": "IfSG Meldedaten", "language": TextLanguage.DE}],
        "contact": [str(Identifier.generate(43))],
        "description": [
            {"value": "Das Infektionsschutzgesetz", "language": TextLanguage.DE}
        ],
        "hasLegalBasis": [
            {
                "language": "de",
                "value": "Infektionsschutzgesetz (IfSG)",
            },
            {
                "language": "en",
                "value": "German Federal Law on the Prevention of Infectious Diseases "
                "(IfSG)",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "keyword": [{"value": "Infektionsschutzgesetz", "language": TextLanguage.DE}],
        "language": ["https://mex.rki.de/item/language-1"],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-3"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Deutschland", "language": TextLanguage.DE}],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [
            {
                "value": "Meldedaten nach Infektionsschutzgesetz (IfSG)",
                "language": TextLanguage.DE,
            }
        ],
        "unitInCharge": [str(Identifier.generate(43))],
    }
    assert extracted_resource.model_dump(exclude_defaults=True) == expected


def test_transform_resource_state_to_mex_resource(
    resource_states: list[ResourceMapping],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_stable_target_ids: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_resources = [
        transform_resource_state_to_mex_resource(
            resource_state,
            extracted_ifsg_resource_parent,
            extracted_primary_sources["ifsg"],
            unit_stable_target_ids,
        )
        for resource_state in resource_states
    ]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": str(extracted_primary_sources["ifsg"].stableTargetId),
        "identifierInPrimarySource": "01",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "alternativeTitle": [
            {"value": "Meldedaten Schleswig-Holstein", "language": TextLanguage.DE}
        ],
        "contact": [str(Identifier.generate(43))],
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "Infektionsschutzgesetz (IfSG)",
            },
            {
                "language": TextLanguage.EN,
                "value": "German Federal Law on the Prevention of Infectious Diseases "
                "(IfSG)",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "isPartOf": [str(extracted_ifsg_resource_parent.stableTargetId)],
        "keyword": [
            {"value": "Infektionsschutzgesetz", "language": TextLanguage.DE},
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-3"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Schleswig-Holstein", "language": TextLanguage.DE}],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Schleswig-Holstein", "language": TextLanguage.DE}],
        "unitInCharge": [str(Identifier.generate(43))],
    }
    assert extracted_resources[0][0].model_dump(exclude_defaults=True) == expected


def test_get_instrument_tool_or_apparatus(
    meta_disease: list[MetaDisease],
    resource_diseases: list[ResourceMapping],
) -> None:
    instrument_tool_or_apparatus = get_instrument_tool_or_apparatus(
        meta_disease[0], resource_diseases[0]
    )

    assert instrument_tool_or_apparatus == [
        Text(value="Falldefinition B", language=TextLanguage.DE),
        Text(value="Falldefinition C", language=TextLanguage.DE),
    ]


def test_transform_resource_disease_to_mex_resource(  # noqa: PLR0913
    resource_diseases: list[ResourceMapping],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_ifsg_resource_state: list[ExtractedResource],
    meta_type: list[MetaType],
    meta_disease: list[MetaDisease],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_stable_target_ids: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    extracted_resource = [
        transform_resource_disease_to_mex_resource(
            resource_disease,
            extracted_ifsg_resource_parent,
            extracted_ifsg_resource_state,
            meta_disease,
            meta_type,
            [101, 102, 103],
            extracted_primary_sources["ifsg"],
            unit_stable_target_ids,
            extracted_organization_rki,
        )
        for resource_disease in resource_diseases
    ]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": str(extracted_primary_sources["ifsg"].stableTargetId),
        "identifierInPrimarySource": "resource_disease_101_1",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "alternativeTitle": [{"value": "ABC"}],
        "contact": [str(Identifier.generate(43))],
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "Infektionsschutzgesetz (IfSG)",
            },
            {
                "language": TextLanguage.EN,
                "value": "German Federal Law on the Prevention of Infectious Diseases "
                "(IfSG)",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "icd10code": ["A1"],
        "instrumentToolOrApparatus": [
            {"value": "Falldefinition B", "language": TextLanguage.DE},
            {"value": "Falldefinition C", "language": TextLanguage.DE},
        ],
        "isPartOf": [
            str(extracted_ifsg_resource_parent.stableTargetId),
            str(extracted_ifsg_resource_state[0].stableTargetId),
            str(extracted_ifsg_resource_state[1].stableTargetId),
        ],
        "keyword": [
            {"value": "virus"},
            {"value": "Epidemic"},
            {"value": "virus"},
            {
                "language": TextLanguage.DE,
                "value": "Infektionsschutzgesetz",
            },
            {
                "language": TextLanguage.DE,
                "value": "Infektionsschutz",
            },
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-3"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Deutschland", "language": TextLanguage.DE}],
        "theme": [
            "https://mex.rki.de/item/theme-11",
        ],
        "title": [
            {
                "language": TextLanguage.DE,
                "value": "Meldedaten nach Infektionsschutzgesetz (IfSG) zu virus (SurvNet Schema 1)",
            }
        ],
        "unitInCharge": [str(Identifier.generate(43))],
    }
    assert extracted_resource[0][0].model_dump(exclude_defaults=True) == expected


def test_transform_ifsg_data_to_mex_variable_group(
    ifsg_variable_group: VariableGroupMapping,
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    meta_field: list[MetaField],
) -> None:
    extracted_variable_group = transform_ifsg_data_to_mex_variable_group(
        ifsg_variable_group,
        extracted_ifsg_resource_disease,
        extracted_primary_sources["ifsg"],
        [meta_field[0]],
        [101],
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["ifsg"].stableTargetId,
        "identifierInPrimarySource": "101_Epi",
        "stableTargetId": Joker(),
        "containedBy": [extracted_ifsg_resource_disease[0].stableTargetId],
        "label": [
            {"value": "Epidemiologische Informationen", "language": TextLanguage.DE}
        ],
    }
    assert extracted_variable_group[0].model_dump(exclude_defaults=True) == expected


def test_transform_ifsg_data_to_mex_variable(  # noqa: PLR0913
    meta_field: list[MetaField],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_ifsg_variable_group: list[ExtractedVariableGroup],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    meta_catalogue2item: list[MetaCatalogue2Item],
    meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    meta_item: list[MetaItem],
    meta_datatype: list[MetaDataType],
    meta_schema2field: list[MetaSchema2Field],
) -> None:
    extracted_variable = transform_ifsg_data_to_mex_variables(
        meta_field,
        extracted_ifsg_resource_disease,
        extracted_ifsg_variable_group,
        extracted_primary_sources["ifsg"],
        meta_catalogue2item,
        meta_catalogue2item2schema,
        meta_item,
        meta_datatype,
        meta_schema2field,
    )

    expected = {
        "identifier": Joker(),
        "hadPrimarySource": str(extracted_primary_sources["ifsg"].stableTargetId),
        "dataType": "DummyType",
        "identifierInPrimarySource": "variable_1_10",
        "stableTargetId": Joker(),
        "belongsTo": [str(extracted_ifsg_variable_group[0].stableTargetId)],
        "description": [{"value": "lokaler"}],
        "label": [
            {"value": "Id der Version (berechneter Wert)", "language": TextLanguage.DE}
        ],
        "usedIn": [str(extracted_ifsg_resource_disease[0].stableTargetId)],
        "valueSet": ["NullItem", "NullItem2"],
    }
    assert extracted_variable[0].model_dump(exclude_defaults=True) == expected

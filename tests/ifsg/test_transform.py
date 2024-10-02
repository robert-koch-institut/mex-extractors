from typing import Any

from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    LinkLanguage,
    MergedOrganizationalUnitIdentifier,
    TextLanguage,
)
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
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
    resource_parent: dict[str, Any],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    unit_stable_target_ids: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_resource = transform_resource_parent_to_mex_resource(
        resource_parent, extracted_primary_sources_ifsg, unit_stable_target_ids
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources_ifsg.stableTargetId,
        "identifierInPrimarySource": "ifsg-parent",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "alternativeTitle": [{"value": "IfSG Meldedaten", "language": TextLanguage.DE}],
        "contact": [Identifier.generate(43)],
        "description": [
            {"value": "Das Infektionsschutzgesetz", "language": TextLanguage.DE}
        ],
        "keyword": [{"value": "Infektionsschutzgesetz", "language": TextLanguage.DE}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publication": [
            {
                "language": LinkLanguage.DE,
                "title": "Infektionsepidemiologisches Jahrbuch",
                "url": "https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
            }
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-1"],
        "resourceTypeSpecific": [{"value": "Meldedaten", "language": TextLanguage.DE}],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Deutschland", "language": TextLanguage.DE}],
        "theme": ["https://mex.rki.de/item/theme-17"],
        "title": [
            {
                "value": "Meldedaten nach Infektionsschutzgesetz (IfSG)",
                "language": TextLanguage.DE,
            }
        ],
        "unitInCharge": [Identifier.generate(43)],
    }
    assert extracted_resource.model_dump(exclude_defaults=True) == expected


def test_transform_resource_state_to_mex_resource(
    resource_states: list[dict[str, Any]],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    unit_stable_target_ids: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    extracted_resources = [
        transform_resource_state_to_mex_resource(
            resource_state,
            extracted_ifsg_resource_parent,
            extracted_primary_sources_ifsg,
            unit_stable_target_ids,
        )
        for resource_state in resource_states
    ]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources_ifsg.stableTargetId,
        "identifierInPrimarySource": "01",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "alternativeTitle": [
            {"value": "Meldedaten Schleswig-Holstein", "language": TextLanguage.DE}
        ],
        "contact": [Identifier.generate(43)],
        "isPartOf": [extracted_ifsg_resource_parent.stableTargetId],
        "keyword": [{"value": "Infektionsschutzgesetz", "language": TextLanguage.DE}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publication": [
            {
                "language": LinkLanguage.DE,
                "title": "Infektionsepidemiologisches Jahrbuch",
                "url": "https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
            },
            {
                "language": LinkLanguage.DE,
                "title": "Epidemiologisches Bulletin",
                "url": "https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
            },
            {
                "language": LinkLanguage.DE,
                "title": "Falldefinitionen",
                "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
            },
            {
                "language": LinkLanguage.DE,
                "title": "Rheinland-Pfalz",
                "url": "http://landesrecht.rlp.de/jportal/portal/page/bsrlpprod.psml?doc.id=jlr-IfSGMeldpflVRPpP1%3Ajuris-lr00&showdoccase=1&doc.hl=1&documentnumber=1",
            },
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-1"],
        "resourceTypeSpecific": [{"value": "Meldedaten", "language": TextLanguage.DE}],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Schleswig-Holstein", "language": TextLanguage.DE}],
        "theme": ["https://mex.rki.de/item/theme-17"],
        "title": [{"value": "Schleswig-Holstein", "language": TextLanguage.DE}],
        "unitInCharge": [Identifier.generate(43)],
    }
    assert extracted_resources[0][0].model_dump(exclude_defaults=True) == expected


def test_get_instrument_tool_or_apparatus(
    meta_disease: list[MetaDisease], resource_diseases: list[dict[str, Any]]
) -> None:
    instrument_tool_or_apparatus = get_instrument_tool_or_apparatus(
        meta_disease[0], resource_diseases[0]
    )
    expected = [
        {"language": "de", "value": "Falldefinition B"},
        {"language": "de", "value": "Falldefinition C"},
    ]
    assert instrument_tool_or_apparatus == expected


def test_transform_resource_disease_to_mex_resource(
    resource_diseases: list[dict[str, Any]],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_ifsg_resource_state: list[ExtractedResource],
    meta_type: list[MetaType],
    meta_disease: list[MetaDisease],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
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
            extracted_primary_sources_ifsg,
            unit_stable_target_ids,
            extracted_organization_rki,
        )
        for resource_disease in resource_diseases
    ]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources_ifsg.stableTargetId,
        "identifierInPrimarySource": "101",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "alternativeTitle": [{"value": "ABC"}],
        "contact": [Identifier.generate(43)],
        "icd10code": ["A1"],
        "instrumentToolOrApparatus": [
            {"value": "Falldefinition B", "language": TextLanguage.DE},
            {"value": "Falldefinition C", "language": TextLanguage.DE},
        ],
        "isPartOf": [
            extracted_ifsg_resource_parent.stableTargetId,
            extracted_ifsg_resource_state[0].stableTargetId,
            extracted_ifsg_resource_state[1].stableTargetId,
        ],
        "keyword": [{"value": "virus"}, {"value": "Epidemic"}, {"value": "virus"}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publication": [
            {
                "language": LinkLanguage.DE,
                "title": "Falldefinitionen",
                "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
            }
        ],
        "publisher": [extracted_organization_rki.stableTargetId],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-1"],
        "resourceTypeSpecific": [{"value": "Meldedaten", "language": TextLanguage.DE}],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Deutschland", "language": TextLanguage.DE}],
        "theme": [
            "https://mex.rki.de/item/theme-17",
            "https://mex.rki.de/item/theme-2",
        ],
        "title": [
            {
                "language": TextLanguage.DE,
                "value": "Meldedaten nach Infektionsschutzgesetz (IfSG) zu virus",
            }
        ],
        "unitInCharge": [Identifier.generate(43)],
    }
    assert extracted_resource[0][0].model_dump(exclude_defaults=True) == expected


def test_transform_ifsg_data_to_mex_variable_group(
    ifsg_variable_group: dict[str, Any],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    meta_field: list[MetaField],
) -> None:
    extracted_variable_group = transform_ifsg_data_to_mex_variable_group(
        ifsg_variable_group,
        extracted_ifsg_resource_disease,
        extracted_primary_sources_ifsg,
        [meta_field[0]],
        [101],
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources_ifsg.stableTargetId,
        "identifierInPrimarySource": "101_Epi",
        "stableTargetId": Joker(),
        "containedBy": [extracted_ifsg_resource_disease[0].stableTargetId],
        "label": [
            {"value": "Epidemiologische Informationen", "language": TextLanguage.DE}
        ],
    }
    assert extracted_variable_group[0].model_dump(exclude_defaults=True) == expected


def test_transform_ifsg_data_to_mex_variable(
    meta_field: list[MetaField],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_ifsg_variable_group: list[ExtractedVariableGroup],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    meta_catalogue2item: list[MetaCatalogue2Item],
    meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    meta_item: list[MetaItem],
) -> None:
    extracted_variable = transform_ifsg_data_to_mex_variables(
        meta_field,
        extracted_ifsg_resource_disease,
        extracted_ifsg_variable_group,
        extracted_primary_sources_ifsg,
        meta_catalogue2item,
        meta_catalogue2item2schema,
        meta_item,
    )

    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources_ifsg.stableTargetId,
        "identifierInPrimarySource": "1",
        "stableTargetId": Joker(),
        "belongsTo": [extracted_ifsg_variable_group[0].stableTargetId],
        "description": [{"value": "lokaler"}],
        "label": [
            {"value": "Id der Version (berechneter Wert)", "language": TextLanguage.DE}
        ],
        "usedIn": [extracted_ifsg_resource_disease[0].stableTargetId],
        "valueSet": ["NullItem", "NullItem2"],
    }
    assert extracted_variable[0].model_dump(exclude_defaults=True) == expected

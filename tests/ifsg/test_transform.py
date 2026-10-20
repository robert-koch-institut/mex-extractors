from typing import TYPE_CHECKING

import pytest

from mex.common.types import (
    Text,
    TextLanguage,
)
from mex.extractors.ifsg.transform import (
    get_instrument_tool_or_apparatus,
    transform_ifsg_data_to_mex_variable_group,
    transform_ifsg_data_to_mex_variables,
    transform_resource_disease_to_mex_resource,
    transform_resource_parent_to_mex_resource,
    transform_resource_state_to_mex_resource,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        ExtractedOrganization,
        ExtractedOrganizationalUnit,
        ExtractedResource,
        ExtractedVariableGroup,
        ResourceMapping,
        VariableGroupMapping,
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


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_resource_parent_to_mex_resource(
    resource_parent: ResourceMapping,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    extracted_resource = transform_resource_parent_to_mex_resource(resource_parent)
    expected = {
        "identifier": "dOJvdpBF6MzHwksxQzSvKA",
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("ifsg")),
        "identifierInPrimarySource": "ifsg-parent",
        "stableTargetId": "hx19mZMrxa85VBJzDZRmqA",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "alternativeTitle": [{"value": "IfSG Meldedaten", "language": TextLanguage.DE}],
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "description": [
            {"value": "Das Infektionsschutzgesetz", "language": TextLanguage.DE}
        ],
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
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
    }
    assert extracted_resource.model_dump(exclude_defaults=True) == expected


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_resource_state_to_mex_resource(
    resource_states: list[ResourceMapping],
    ifsg_extracted_resource_parent: ExtractedResource,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    extracted_resources = [
        transform_resource_state_to_mex_resource(
            resource_state,
            ifsg_extracted_resource_parent,
        )
        for resource_state in resource_states
    ]
    expected = {
        "identifier": "L1t28UtnwFZnbjA1amAFA",
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("ifsg")),
        "identifierInPrimarySource": "01",
        "stableTargetId": "ezyCT9U2ZhjReYg56DJ83k",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "alternativeTitle": [
            {"value": "Meldedaten Schleswig-Holstein", "language": TextLanguage.DE}
        ],
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "description": [
            {"language": TextLanguage.DE, "value": "Das Infektionsschutzgesetz"}
        ],
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
        "keyword": [
            {"value": "Infektionsschutzgesetz", "language": TextLanguage.DE},
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "relatedResource": [str(ifsg_extracted_resource_parent.stableTargetId)],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-3"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "rights": [{"value": "Gesundheitsdaten.", "language": TextLanguage.DE}],
        "spatial": [{"value": "Schleswig-Holstein", "language": TextLanguage.DE}],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Schleswig-Holstein", "language": TextLanguage.DE}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
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


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_resource_disease_to_mex_resource(  # noqa: PLR0913, PLR0917
    resource_diseases: list[ResourceMapping],
    ifsg_extracted_resource_parent: ExtractedResource,
    ifsg_extracted_resources_state: list[ExtractedResource],
    meta_type: list[MetaType],
    meta_disease: list[MetaDisease],
    extracted_organization_rki: ExtractedOrganization,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    extracted_resource = [
        transform_resource_disease_to_mex_resource(
            resource_disease,
            ifsg_extracted_resource_parent,
            ifsg_extracted_resources_state,
            meta_disease,
            meta_type,
            [101, 102, 103],
            extracted_organization_rki,
        )
        for resource_disease in resource_diseases
    ]
    expected = {
        "identifier": "dQxBvVZWlL5pUrEh7U2oIn",
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("ifsg")),
        "identifierInPrimarySource": "resource_disease_101_1",
        "stableTargetId": "dv4MYrpFnn1akbYp4zdwvf",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "alternativeTitle": [{"value": "ABC"}],
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "description": [
            {"value": "Das Infektionsschutzgesetz", "language": TextLanguage.DE}
        ],
        "hasCodeValues": [{"value": "A1"}],
        "hasCodingSystem": ["https://mex.rki.de/item/coding-system-1"],
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
        "instrumentToolOrApparatus": [
            {"value": "Falldefinition B", "language": TextLanguage.DE},
            {"value": "Falldefinition C", "language": TextLanguage.DE},
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
        "relatedResource": [
            str(ifsg_extracted_resource_parent.stableTargetId),
            str(ifsg_extracted_resources_state[0].stableTargetId),
            str(ifsg_extracted_resources_state[1].stableTargetId),
        ],
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
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
    }
    assert extracted_resource[0][0].model_dump(exclude_defaults=True) == expected


def test_transform_ifsg_data_to_mex_variable_group(
    ifsg_variable_group: VariableGroupMapping,
    ifsg_extracted_resources_disease: list[ExtractedResource],
    meta_field: list[MetaField],
) -> None:
    extracted_variable_group = transform_ifsg_data_to_mex_variable_group(
        ifsg_variable_group,
        ifsg_extracted_resources_disease,
        [meta_field[0]],
        [101],
    )
    expected = {
        "identifier": "dy76Pr7t5xk1AN9q1VrPze",
        "hadPrimarySource": get_extracted_primary_source_id_by_name("ifsg"),
        "identifierInPrimarySource": "101_Epi",
        "stableTargetId": "cp53EWTlPaIwyReswGti1B",
        "containedBy": [ifsg_extracted_resources_disease[0].stableTargetId],
        "label": [
            {"value": "Epidemiologische Informationen", "language": TextLanguage.DE}
        ],
    }
    assert extracted_variable_group[0].model_dump(exclude_defaults=True) == expected


def test_transform_ifsg_data_to_mex_variable(  # noqa: PLR0913, PLR0917
    meta_field: list[MetaField],
    ifsg_extracted_resources_disease: list[ExtractedResource],
    ifsg_extracted_variable_groups: list[ExtractedVariableGroup],
    meta_catalogue2item: list[MetaCatalogue2Item],
    meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    meta_item: list[MetaItem],
    meta_datatype: list[MetaDataType],
    meta_schema2field: list[MetaSchema2Field],
) -> None:
    extracted_variable = transform_ifsg_data_to_mex_variables(
        meta_field,
        ifsg_extracted_resources_disease,
        ifsg_extracted_variable_groups,
        meta_catalogue2item,
        meta_catalogue2item2schema,
        meta_item,
        meta_datatype,
        meta_schema2field,
    )

    expected = {
        "identifier": "hPtgPF3YUENiCDlUpXKS05",
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("ifsg")),
        "dataType": "DummyType",
        "identifierInPrimarySource": "variable_1_10",
        "stableTargetId": "bPeWOlnPLtLyN2yQhA9pXH",
        "belongsTo": [str(ifsg_extracted_variable_groups[0].stableTargetId)],
        "description": [{"value": "lokaler"}],
        "label": [
            {"value": "Id der Version (berechneter Wert)", "language": TextLanguage.DE}
        ],
        "usedIn": [str(ifsg_extracted_resources_disease[0].stableTargetId)],
        "valueSet": ["NullItem", "NullItem2"],
    }
    assert extracted_variable[0].model_dump(exclude_defaults=True) == expected

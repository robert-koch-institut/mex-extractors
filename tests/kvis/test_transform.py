from typing import TYPE_CHECKING

import pytest

from mex.common.types import MergedResourceIdentifier, TextLanguage
from mex.extractors.kvis.transform import (
    transform_kvis_fieldvalues_table_entries_to_setvalues,
    transform_kvis_resource_to_extracted_resource,
    transform_kvis_table_entries_to_extracted_variables,
    transform_kvis_variables_to_extracted_variable_groups,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        ExtractedContactPoint,
        ExtractedOrganization,
        ExtractedOrganizationalUnit,
        ExtractedPerson,
        ExtractedVariableGroup,
    )
    from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_kvis_resource_to_extracted_resource(
    contact_point: ExtractedContactPoint,
    juturna_felicitas: ExtractedPerson,
    frieda_fictitious: ExtractedPerson,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
    wikidata_extracted_organization: ExtractedOrganization,
) -> None:
    extracted_resource = transform_kvis_resource_to_extracted_resource()
    assert extracted_resource.model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [{"value": "KVIS", "language": TextLanguage.DE}],
        "contact": [contact_point.stableTargetId],
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "contributor": [
            juturna_felicitas.stableTargetId,
            frieda_fictitious.stableTargetId,
        ],
        "description": [{"value": "Wörter", "language": TextLanguage.DE}],
        "documentation": [
            {"language": TextLanguage.DE, "title": "a", "url": "http://www.a.b"},
            {"language": TextLanguage.DE, "title": "c", "url": "http://www.c.d"},
        ],
        "externalPartner": [wikidata_extracted_organization.stableTargetId],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("kvis"),
        "hasCodingSystem": ["https://mex.rki.de/item/coding-system-1"],
        "healthCategory": ["https://mex.rki.de/item/health-category-1"],
        "identifierInPrimarySource": "kvis_resource",
        "keyword": [{"value": "Schlüsselwort", "language": TextLanguage.DE}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [wikidata_extracted_organization.stableTargetId],
        "start": ["1999"],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Titel", "language": TextLanguage.DE}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
    }


def test_transform_kvis_variables_to_extracted_variable_groups(
    mocked_kvisvariables: list[KVISVariables],
    mocked_extracted_resource_id: MergedResourceIdentifier,
) -> None:
    extracted_variable_group = transform_kvis_variables_to_extracted_variable_groups(
        mocked_extracted_resource_id, mocked_kvisvariables
    )
    assert len(extracted_variable_group) == 2
    assert extracted_variable_group[1].model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "containedBy": [str(mocked_extracted_resource_id)],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("kvis"),
        "identifierInPrimarySource": "kvis_file with strings and bools",
        "label": [
            {"language": TextLanguage.DE, "value": "file with strings and bools"}
        ],
    }


def test_transform_kvis_fieldvalues_table_entries_to_setvalues(
    mocked_kvisfieldvalues: list[KVISFieldValues],
) -> None:
    valueset_dict = transform_kvis_fieldvalues_table_entries_to_setvalues(
        mocked_kvisfieldvalues
    )
    assert valueset_dict == {
        "STRING": ["one", "two", "three"],
        "BOOL": ["it is false", "it is true"],
    }


def test_transform_kvis_table_entries_to_extracted_variables(
    mocked_extracted_resource_id: MergedResourceIdentifier,
    mocked_extracted_variable_groups: list[ExtractedVariableGroup],
    mocked_kvisvariables: list[KVISVariables],
    mocked_valuesets_by_variable_name: dict[str, list[str]],
) -> None:
    extracted_variables = transform_kvis_table_entries_to_extracted_variables(
        mocked_extracted_resource_id,
        mocked_extracted_variable_groups,
        mocked_kvisvariables,
        mocked_valuesets_by_variable_name,
    )
    assert len(extracted_variables) == 3
    assert extracted_variables[0].model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "belongsTo": [mocked_extracted_variable_groups[0].stableTargetId],
        "dataType": "integer field",
        "description": [{"value": "some integer field", "language": TextLanguage.DE}],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("kvis"),
        "identifierInPrimarySource": "kvis_int",
        "label": [{"value": "Integer", "language": TextLanguage.DE}],
        "usedIn": ["bFQoRhcVH5DK7x"],
    }

    assert extracted_variables[2].model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "belongsTo": [mocked_extracted_variable_groups[1].stableTargetId],
        "dataType": "bool field",
        "description": [
            {"language": TextLanguage.DE, "value": "a boolean field for flagging"}
        ],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("kvis"),
        "identifierInPrimarySource": "kvis_bool",
        "label": [{"value": "boolean", "language": TextLanguage.DE}],
        "usedIn": ["bFQoRhcVH5DK7x"],
        "valueSet": ["it is false", "it is true"],
    }

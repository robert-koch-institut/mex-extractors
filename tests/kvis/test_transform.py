from typing import TYPE_CHECKING

import pytest

from mex.common.types import MergedResourceIdentifier, TextLanguage
from mex.extractors.kvis.transform import (
    lookup_kvis_functional_account_in_ldap_and_transform,
    transform_kvis_fieldvalues_table_entries_to_setvalues,
    transform_kvis_resource_to_extracted_resource,
    transform_kvis_table_entries_to_extracted_variables,
    transform_kvis_variables_to_extracted_variable_groups,
)

if TYPE_CHECKING:
    from mex.common.models import (
        ExtractedContactPoint,
        ExtractedVariableGroup,
    )
    from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables


@pytest.mark.usefixtures("mocked_ldap")
def test_lookup_kvis_functional_account_in_ldap_and_transform(
    contact_point: ExtractedContactPoint,
) -> None:
    contact_id = lookup_kvis_functional_account_in_ldap_and_transform(
        contact_point.email[0],
    )
    assert contact_id == contact_point.stableTargetId


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_kvis_resource_to_extracted_resource() -> None:
    extracted_resource = transform_kvis_resource_to_extracted_resource()
    assert extracted_resource.model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_resource",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "created": "1999",
        "contact": ["cMkmnNOoNVAohBA1XLNr9K"],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Titel", "language": "de"}],
        "unitInCharge": ["bFQoRhcVH5IS5j"],
        "alternativeTitle": [{"value": "KVIS", "language": "de"}],
        "contributingUnit": ["bFQoRhcVH5IS5j"],
        "contributor": ["cpKNwpoZTQ4GpIzBgO8DMx", "c2Yd8aNoLKIf7u6ubTUuc3"],
        "description": [{"value": "Wörter", "language": "de"}],
        "documentation": [
            {"language": "de", "title": "a", "url": "http://www.a.b"},
            {"language": "de", "title": "c", "url": "http://www.c.d"},
        ],
        "externalPartner": ["ga6xh6pgMwgq7DC7r6Wjqg"],
        "keyword": [{"value": "Schlüsselwort", "language": "de"}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": ["ga6xh6pgMwgq7DC7r6Wjqg"],
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
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
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
        "belongsTo": ["CooqBBvfd2q077RJ1qc1S"],
        "dataType": "integer field",
        "description": [{"value": "some integer field", "language": TextLanguage.DE}],
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_int",
        "label": [{"value": "Integer", "language": TextLanguage.DE}],
        "usedIn": ["bFQoRhcVH5DK7x"],
    }

    assert extracted_variables[2].model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "belongsTo": ["gSCYeMkhBrsWdo8Xoei8bk"],
        "dataType": "bool field",
        "description": [
            {"language": TextLanguage.DE, "value": "a boolean field for flagging"}
        ],
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_bool",
        "label": [{"value": "boolean", "language": TextLanguage.DE}],
        "usedIn": ["bFQoRhcVH5DK7x"],
        "valueSet": ["it is false", "it is true"],
    }

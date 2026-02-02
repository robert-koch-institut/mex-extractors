import pytest
from mex.common.models import ExtractedVariableGroup

from mex.common.types import LinkLanguage, MergedResourceIdentifier, TextLanguage
from mex.extractors.kvis.models.table_models import KVISVariables, KVISFieldValues
from mex.extractors.kvis.transform import (
    transform_kvis_resource_to_extracted_resource,
    transform_kvis_variables_to_extracted_variable_groups,
    transform_kvis_table_entries_to_extracted_variables,
)


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_kvis_resource_to_extracted_resource() -> None:
    extracted_resource = transform_kvis_resource_to_extracted_resource()
    assert extracted_resource.model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [{"language": TextLanguage.DE, "value": "KVIS"}],
        "contact": ["bFQoRhcVH5DIek"],
        "contributingUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "contributor": ["bFQoRhcVH5DIek", "bFQoRhcVH5DIek"],
        "created": "1999",
        "description": [{"language": TextLanguage.DE, "value": "Wörter"}],
        "documentation": [
            {"language": LinkLanguage.DE, "title": "a", "url": "http://www.a.b"},
            {"language": LinkLanguage.DE, "title": "c", "url": "http://www.c.d"},
        ],
        "externalPartner": ["ga6xh6pgMwgq7DC7r6Wjqg"],
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_resource",
        "keyword": [{"language": TextLanguage.DE, "value": "Schlüsselwort"}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": ["ga6xh6pgMwgq7DC7r6Wjqg"],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"language": TextLanguage.DE, "value": "Titel"}],
        "unitInCharge": ["cjna2jitPngp6yIV63cdi9"],
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
        "identifierInPrimarySource": "kvis_another file type",
        "label": [{"language": TextLanguage.DE, "value": "another file type"}],
    }


def test_transform_kvis_table_entries_to_extracted_variables(
    mocked_extracted_resource_id: MergedResourceIdentifier,
    mocked_extracted_variable_groups: list[ExtractedVariableGroup],
    mocked_kvisvariables: list[KVISVariables],
    mocked_kvisfieldvalues: list[KVISFieldValues],
) -> None:
    extracted_variables = transform_kvis_table_entries_to_extracted_variables(
        mocked_extracted_resource_id,
        mocked_extracted_variable_groups,
        mocked_kvisvariables,
        mocked_kvisfieldvalues,
    )
    assert len(extracted_variables) == 3
    assert extracted_variables[0].model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "belongsTo": ["hxWBV2djsXmw3fNmkrh8S2"],
        "dataType": "integer",
        "description": [{"value": "field description"}],
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_field name short",
        "label": [{"language": TextLanguage.EN, "value": "field name long"}],
        "usedIn": ["bFQoRhcVH5DK7x"]
    }

    assert extracted_variables[2].model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "belongsTo": ["hNI0nop3NLG8VWawi91Rti"],
        "dataType": "bool",
        "description": [
            {"language": TextLanguage.EN,"value": "a boolean field for flagging"}
        ],
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_bit",
        "label": [{"value": "bool"}],
        "usedIn": ["bFQoRhcVH5DK7x"],
        "valueSet": ["it is true", "it is false"]
    }
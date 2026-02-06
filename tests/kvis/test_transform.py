import pytest

from mex.common.models import (
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedVariableGroup,
)
from mex.common.types import LinkLanguage, MergedResourceIdentifier, TextLanguage
from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables
from mex.extractors.kvis.transform import (
    lookup_kvis_functional_account_in_ldap_and_transform,
    lookup_kvis_person_in_ldap_and_transform,
    transform_kvis_fieldvalues_table_entries_to_setvalues,
    transform_kvis_resource_to_extracted_resource,
    transform_kvis_table_entries_to_extracted_variables,
    transform_kvis_variables_to_extracted_variable_groups,
)


@pytest.mark.usefixtures("mocked_ldap")
def test_lookup_kvis_person_in_ldap_and_transform(
    juturna_felicitas: ExtractedPerson,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    person_id = lookup_kvis_person_in_ldap_and_transform(
        juturna_felicitas.email[0],
        mocked_units_by_identifier_in_primary_source,
        extracted_organization_rki,
    )
    assert person_id == juturna_felicitas.stableTargetId


@pytest.mark.usefixtures("mocked_ldap")
def test_lookup_kvis_functional_account_in_ldap_and_transform(
    contact_point: ExtractedContactPoint,
) -> None:
    contact_id = lookup_kvis_functional_account_in_ldap_and_transform(
        contact_point.email[0],
    )
    assert contact_id == contact_point.stableTargetId


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_kvis_resource_to_extracted_resource(
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
    contact_point: ExtractedContactPoint,
    juturna_felicitas: ExtractedPerson,
    frieda_fictitious: ExtractedPerson,
) -> None:
    extracted_resource = transform_kvis_resource_to_extracted_resource(
        mocked_extracted_organizational_units,
        extracted_organization_rki,
    )
    assert extracted_resource.model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [{"language": TextLanguage.DE, "value": "KVIS"}],
        "contact": [contact_point.stableTargetId],
        "contributingUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "contributor": [
            juturna_felicitas.stableTargetId,
            frieda_fictitious.stableTargetId,
        ],
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

from collections import defaultdict
from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_functional_account_to_extracted_contact_point,
    transform_ldap_person_to_extracted_person,
)
from mex.common.models import (
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedPersonIdentifier,
    MergedResourceIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables


def lookup_kvis_person_in_ldap_and_transform(
    person_email: str,
    units_by_identifier_in_primary_source: dict[str, ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> MergedPersonIdentifier | None:
    """Lookup person in ldap, transform to ExtractedPerson, load, and return stable ID.

    Args:
        person_email: email of person,
        units_by_identifier_in_primary_source: dict of primary sources by ID
        extracted_organization_rki: RKI extracted organization

    Returns:
        MergedPersonIdentifier if matched or None if match fails
    """
    ldap = LDAPConnector.get()
    try:
        ldap_person = ldap.get_person(mail=person_email)
        extracted_person = transform_ldap_person_to_extracted_person(
            ldap_person,
            get_extracted_primary_source_id_by_name("ldap"),
            units_by_identifier_in_primary_source,
            extracted_organization_rki,
        )
    except MExError:
        return None
    else:
        load([extracted_person])
        return extracted_person.stableTargetId


def lookup_kvis_functional_account_in_ldap_and_transform(
    mail: str,
) -> MergedContactPointIdentifier | None:
    """Lookup a functional email in ldap, transform to extracted contact point, load.

    Args:
        mail: email of functional account,

    Returns:
        MergedContactPointIdentifier if matched or None if match fails
    """
    ldap = LDAPConnector.get()
    try:
        ldap_contact = ldap.get_functional_account(mail=mail)
        extracted_contact_point = (
            transform_ldap_functional_account_to_extracted_contact_point(
                ldap_contact,
                get_extracted_primary_source_id_by_name("ldap"),
            )
        )
    except MExError:
        return None
    else:
        load([extracted_contact_point])
        return extracted_contact_point.stableTargetId


def transform_kvis_resource_to_extracted_resource(
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> ExtractedResource:
    """Transform the Resource mapping to an extracted resource. No reading from KVIS.

    Args:
        extracted_organizational_units: list of extracted organizational units,
        extracted_organization_rki: RKI extracted organizational unit

    Returns:
        ExtractedResource
    """
    settings = Settings.get()
    mapping = ResourceMapping.model_validate(
        load_yaml(settings.kvis.mapping_path / "resource.yaml")
    )

    units_by_identifier_in_primary_source = {
        unit.identifierInPrimarySource: unit for unit in extracted_organizational_units
    }

    contact = (
        [
            lookup_kvis_functional_account_in_ldap_and_transform(c)
            for c in mapping.contact[0].mappingRules[0].forValues
        ]
        if mapping.contact[0].mappingRules[0].forValues
        else []
    )
    contributing_unit = (
        get_unit_merged_id_by_synonym(
            mapping.contributingUnit[0].mappingRules[0].forValues[0]
        )
        if mapping.contributingUnit[0].mappingRules[0].forValues
        else None
    )
    contributor = (
        [
            lookup_kvis_person_in_ldap_and_transform(
                c, units_by_identifier_in_primary_source, extracted_organization_rki
            )
            for c in mapping.contributor[0].mappingRules[0].forValues
        ]
        if mapping.contributor[0].mappingRules[0].forValues
        else []
    )
    external_partner = (
        [partner_id]
        if mapping.externalPartner[0].mappingRules[0].forValues
        and (
            partner_id := get_wikidata_extracted_organization_id_by_name(
                mapping.externalPartner[0].mappingRules[0].forValues[0]
            )
        )
        else []
    )
    publisher = (
        get_wikidata_extracted_organization_id_by_name(
            mapping.publisher[0].mappingRules[0].forValues[0]
        )
        if mapping.publisher[0].mappingRules[0].forValues
        else None
    )
    unit_in_charge = (
        get_unit_merged_id_by_synonym(
            mapping.unitInCharge[0].mappingRules[0].forValues[0]
        )
        if mapping.unitInCharge[0].mappingRules[0].forValues
        else None
    )

    return ExtractedResource(
        accessRestriction=mapping.accessRestriction[0].mappingRules[0].setValues,
        accrualPeriodicity=mapping.accrualPeriodicity[0].mappingRules[0].setValues,
        alternativeTitle=mapping.alternativeTitle[0].mappingRules[0].setValues,
        contact=contact,
        contributingUnit=contributing_unit,
        contributor=contributor,
        created=mapping.created[0].mappingRules[0].setValues,
        description=mapping.description[0].mappingRules[0].setValues,
        documentation=mapping.documentation[0].mappingRules[0].setValues,
        externalPartner=external_partner,
        hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
        hasLegalBasis=mapping.hasLegalBasis[0].mappingRules[0].setValues,
        hasPurpose=mapping.hasPurpose[0].mappingRules[0].setValues,
        identifierInPrimarySource=mapping.identifierInPrimarySource[0]
        .mappingRules[0]
        .setValues,
        keyword=mapping.keyword[0].mappingRules[0].setValues,
        language=mapping.language[0].mappingRules[0].setValues,
        method=mapping.method[0].mappingRules[0].setValues,
        populationCoverage=mapping.populationCoverage[0].mappingRules[0].setValues,
        provenance=mapping.provenance[0].mappingRules[0].setValues,
        publisher=[publisher],
        resourceCreationMethod=mapping.resourceCreationMethod[0]
        .mappingRules[0]
        .setValues,
        resourceTypeGeneral=mapping.resourceTypeGeneral[0].mappingRules[0].setValues,
        resourceTypeSpecific=mapping.resourceTypeSpecific[0].mappingRules[0].setValues,
        spatial=mapping.spatial[0].mappingRules[0].setValues,
        theme=mapping.theme[0].mappingRules[0].setValues,
        title=mapping.title[0].mappingRules[0].setValues,
        unitInCharge=unit_in_charge,
    )


def transform_kvis_variables_to_extracted_variable_groups(
    kvis_extracted_resource_id: MergedResourceIdentifier,
    kvis_variables_table_entries: list[KVISVariables],
) -> list[ExtractedVariableGroup]:
    """Transform entries of the kvis variables table to extracted variable groups.

    Args:
        kvis_extracted_resource_id: MergedResourceIdentifier of kvis resource
        kvis_variables_table_entries: The kvis variables table entries.

    Returns:
        list of extracted variable group entries.
    """
    unique_file_types = sorted(
        {item.file_type for item in kvis_variables_table_entries}
    )
    return [
        ExtractedVariableGroup(
            containedBy=[kvis_extracted_resource_id],
            hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
            identifierInPrimarySource=f"kvis_{file_type}",
            label=[Text(value=file_type, language=TextLanguage.DE)],
        )
        for file_type in unique_file_types
    ]


def transform_kvis_fieldvalues_table_entries_to_setvalues(
    kvis_fieldvalues_table_entries: list[KVISFieldValues],
) -> dict[str, list[str]]:
    """Collect valueSets from fieldvalues table according to mapping rules.

    Args:
        kvis_fieldvalues_table_entries: list of KVISFieldValue table entries

    Returns:
        dictionary of setValues by variable
    """
    settings = Settings.get()
    variable_mapping = VariableMapping.model_validate(
        load_yaml(settings.kvis.mapping_path / "variable.yaml")
    )

    # get variable names by lookup field for valueset collection:
    valueset_variables_by_lookupfield = {
        rule.fieldInPrimarySource: rule.mappingRules[0].forValues
        for rule in variable_mapping.valueSet
    }

    # Collect valueSets from table per variable according to lookup field
    valuesets_by_variable_name: dict[str, list[str]] = defaultdict(list)
    for item in kvis_fieldvalues_table_entries:
        if (
            valueset_variables_by_lookupfield["FieldValue"]
            and item.field_value_list_name
            in valueset_variables_by_lookupfield["FieldValue"]
        ):
            valuesets_by_variable_name[item.field_value_list_name].append(
                item.field_value
            )
        elif (
            valueset_variables_by_lookupfield["FieldValueLongText"]
            and item.field_value_list_name
            in valueset_variables_by_lookupfield["FieldValueLongText"]
        ):
            valuesets_by_variable_name[item.field_value_list_name].append(
                item.field_value_long_text
            )
        else:
            msg = (
                f"no lookup-field (fieldInPrimarySource) defined in variables.yaml "
                f"for 'valueSet' for KVIS field '{item.field_value_list_name}'."
            )
            raise MExError(msg)
    return valuesets_by_variable_name


def transform_kvis_table_entries_to_extracted_variables(
    kvis_extracted_resource_id: MergedResourceIdentifier,
    kvis_extracted_variable_groups: list[ExtractedVariableGroup],
    kvis_variables_table_entries: list[KVISVariables],
    kvis_valuesets_by_variable_name: dict[str, list[str]],
) -> list[ExtractedVariable]:
    """Transform entries of the kvis tables to extracted variables.

    Args:
        kvis_extracted_resource_id: MergedResourceIdentifier of kvis resource
        kvis_extracted_variable_groups: list of ExtractedVariableGroups
        kvis_variables_table_entries: list of KVISVariables table entries
        kvis_valuesets_by_variable_name: value sets for variables by variable name.

    Returns:
        list of ExtractedVariables
    """
    extracted_variable_group_id_by_label = {
        item.label[0].value: item.stableTargetId
        for item in kvis_extracted_variable_groups
    }

    return [
        ExtractedVariable(
            belongsTo=[extracted_variable_group_id_by_label[item.file_type]],
            dataType=item.datatype_description,
            description=[Text(value=item.field_description, language=TextLanguage.DE)],
            hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
            identifierInPrimarySource=f"kvis_{item.field_name_short}",
            label=[Text(value=item.field_name_long, language=TextLanguage.DE)],
            usedIn=[kvis_extracted_resource_id],
            valueSet=kvis_valuesets_by_variable_name.get(item.fvlist_name)
            if item.fvlist_name
            else None,
        )
        for item in kvis_variables_table_entries
    ]

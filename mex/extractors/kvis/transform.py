from collections import defaultdict
from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.common.models import (
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedResourceIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.assets import load_yaml
from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.kvis.models.table_models import KVISFieldValues, KVISVariables


def transform_kvis_resource_to_extracted_resource() -> ExtractedResource:
    """Transform the Resource mapping to an extracted resource. No reading from KVIS.

    Args:
        extracted_organizational_units: list of extracted organizational units,
        extracted_organization_rki: RKI extracted organizational unit

    Returns:
        ExtractedResource
    """
    settings = ExtractorsSettings.get()
    mapping = ResourceMapping.model_validate(
        load_yaml(f"{settings.kvis.mapping_path}/resource.yaml")
    )

    contact = [
        get_ldap_merged_contact_id_by_mail(mail=c)
        for c in mapping.contact.get_mappable_values()
    ]
    contributing_unit = (
        get_unit_merged_id_by_synonym(mapping.contributingUnit.get_mappable_values()[0])
        if mapping.contributingUnit.get_mappable_values()
        else None
    )
    contributor = [
        get_ldap_merged_person_id_by_query(mail=c)
        for c in mapping.contributor.get_mappable_values()
    ]

    external_partner = (
        [partner_id]
        if mapping.externalPartner.get_mappable_values()
        and (
            partner_id := get_wikidata_extracted_organization_id_by_name(
                mapping.externalPartner.get_mappable_values()[0]
            )
        )
        else []
    )
    publisher = (
        get_wikidata_extracted_organization_id_by_name(
            mapping.publisher.get_mappable_values()[0]
        )
        if mapping.publisher.get_mappable_values()
        else None
    )
    unit_in_charge = (
        get_unit_merged_id_by_synonym(mapping.unitInCharge.get_mappable_values()[0])
        if mapping.unitInCharge.get_mappable_values()
        else None
    )

    return ExtractedResource(
        accessRestriction=mapping.accessRestriction.get_static_values(),
        accrualPeriodicity=mapping.accrualPeriodicity.get_static_values(),
        alternativeTitle=mapping.alternativeTitle.get_static_values(),
        contact=contact,
        contributingUnit=contributing_unit,
        contributor=contributor,
        description=mapping.description.get_static_values(),
        documentation=mapping.documentation.get_static_values(),
        externalPartner=external_partner,
        hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
        hasCodingSystem=mapping.hasCodingSystem.get_static_values(),
        hasLegalBasis=mapping.hasLegalBasis.get_static_values(),
        hasPurposeDescription=mapping.hasPurposeDescription.get_static_values(),
        healthCategory=mapping.healthCategory.get_static_values(),
        identifierInPrimarySource=mapping.identifierInPrimarySource.get_static_values(),
        keyword=mapping.keyword.get_static_values(),
        language=mapping.language.get_static_values(),
        method=mapping.method.get_static_values(),
        populationCoverage=mapping.populationCoverage.get_static_values(),
        provenance=mapping.provenance.get_static_values(),
        publisher=[publisher],
        resourceCreationMethod=mapping.resourceCreationMethod.get_static_values(),
        resourceTypeGeneral=mapping.resourceTypeGeneral.get_static_values(),
        resourceTypeSpecific=mapping.resourceTypeSpecific.get_static_values(),
        spatial=mapping.spatial.get_static_values(),
        start=mapping.start.get_static_values(),
        theme=mapping.theme.get_static_values(),
        title=mapping.title.get_static_values(),
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
    settings = ExtractorsSettings.get()
    variable_mapping = VariableMapping.model_validate(
        load_yaml(f"{settings.kvis.mapping_path}/variable.yaml")
    )

    # get variable names by lookup field for valueset collection:
    field_value_variables = variable_mapping.valueSet.get_mappable_values(
        field_in_primary_source="FieldValue"
    )
    field_value_long_text_variables = variable_mapping.valueSet.get_mappable_values(
        field_in_primary_source="FieldValueLongText"
    )

    # Collect valueSets from table per variable according to lookup field
    valuesets_by_variable_name: dict[str, list[str]] = defaultdict(list)
    for item in kvis_fieldvalues_table_entries:
        if item.field_value_list_name in field_value_variables:
            valuesets_by_variable_name[item.field_value_list_name].append(
                item.field_value
            )
        elif item.field_value_list_name in field_value_long_text_variables:
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

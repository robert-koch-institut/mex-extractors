from typing import TYPE_CHECKING

from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
    VariableMapping,
)
from mex.extractors.odk.filter import is_invalid_odk_variable
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from mex.common.types import (
        MergedOrganizationIdentifier,
    )
    from mex.extractors.odk.model import ODKData


def transform_odk_resources_to_mex_resources(
    odk_resource_mappings: list[ResourceMapping],
    odk_merged_organization_ids_by_str: dict[str, MergedOrganizationIdentifier],
    international_projects_extracted_activities: list[ExtractedActivity],
) -> tuple[list[ExtractedResource], list[ExtractedResource]]:
    """Transform odk resources to mex resources.

    Args:
        odk_resource_mappings: list of resource mapping models
        international_projects_extracted_primary_source: primary source
        odk_merged_organization_ids_by_str: dict of wikidata OrganizationIDs
        international_projects_extracted_activities: list of extracted international
                                                     projects activities

    Returns:
        tuple of list of mex child and non-child resources
    """
    international_projects_stable_target_id_by_identifier_in_primary_source = {
        activity.identifierInPrimarySource: activity.stableTargetId
        for activity in international_projects_extracted_activities
    }
    resources_tuple: tuple[list[ExtractedResource], list[ExtractedResource]] = ([], [])
    for resource in odk_resource_mappings:
        alternative_title = resource.alternativeTitle.get_static_values()
        contributing_unit = [
            unit_id
            for synonym in resource.contributingUnit.get_mappable_values()
            if (unit_ids := get_unit_merged_id_by_synonym(synonym))
            for unit_id in unit_ids
        ]
        description = resource.description.get_static_values()
        has_legal_basis = resource.hasLegalBasis.get_static_values()
        identifier_in_primary_source = (
            resource.identifierInPrimarySource.get_static_values()
        )
        method_description = resource.methodDescription.get_static_values()
        number_of_unique_individuals = (
            resource.numberOfUniqueIndividuals.get_static_values()
        )

        was_generated_by = (
            international_projects_stable_target_id_by_identifier_in_primary_source[
                resource.wasGeneratedBy.get_mappable_values()[0]
            ]
        )
        external_partner: list[MergedOrganizationIdentifier] = []
        for partner in resource.externalPartner.get_mappable_values():
            if partner in odk_merged_organization_ids_by_str:
                external_partner.append(odk_merged_organization_ids_by_str[partner])
            else:
                organization = ExtractedOrganization(
                    identifierInPrimarySource=partner,
                    officialName=partner,
                    hadPrimarySource=get_extracted_primary_source_id_by_name("mex"),
                )
                load([organization])
                external_partner.append(organization.stableTargetId)
        publisher = [
            partner
            for name in resource.publisher.get_mappable_values()
            if (partner := odk_merged_organization_ids_by_str.get(name))  # type: ignore[assignment]
        ]
        resources_tuple[bool(resource.relatedResource)].append(
            ExtractedResource(
                identifierInPrimarySource=identifier_in_primary_source,
                accessRestriction=resource.accessRestriction.get_static_values(),
                alternativeTitle=alternative_title,
                contact=get_unit_merged_id_by_synonym(
                    resource.contact.get_mappable_values()[0]
                ),
                contributingUnit=contributing_unit,
                description=description,
                end=resource.end.get_static_values(),
                externalPartner=external_partner,
                hadPrimarySource=get_extracted_primary_source_id_by_name("odk"),
                hasLegalBasis=has_legal_basis,
                healthCategory=resource.healthCategory.get_static_values(),
                keyword=resource.keyword.get_static_values(),
                language=resource.language.get_static_values(),
                meshId=resource.meshId.get_static_values(),
                method=resource.method.get_static_values(),
                methodDescription=method_description,
                numberOfUniqueIndividuals=number_of_unique_individuals,
                publisher=publisher,
                resourceCreationMethod=resource.resourceCreationMethod.get_static_values(),
                resourceTypeGeneral=resource.resourceTypeGeneral.get_static_values(),
                resourceTypeSpecific=resource.resourceTypeSpecific.get_static_values(),
                rights=resource.rights.get_static_values(),
                spatial=resource.spatial.get_static_values(),
                start=resource.start.get_static_values(),
                theme=resource.theme.get_static_values(),
                title=resource.title.get_static_values(),
                unitInCharge=get_unit_merged_id_by_synonym(
                    resource.unitInCharge.get_mappable_values()[0]
                ),
                wasGeneratedBy=was_generated_by,
            )
        )
    return resources_tuple


def assign_resource_relations_and_load(
    resources_tuple: tuple[list[ExtractedResource], list[ExtractedResource]],
) -> list[ExtractedResource]:
    """Assign resources related to each other.

    Args:
        resources_tuple : tuple of list of mex resources

    Returns:
        list of mex resources
    """
    main_questionnaire_id = next(
        resource
        for resource in resources_tuple[0]
        if resource.identifierInPrimarySource
        == "BCHW_ZIG2_FG37_main_questionnaire_01052021"
    ).stableTargetId
    load(resources_tuple[0])
    for resource in resources_tuple[1]:
        resource.relatedResource = [main_questionnaire_id]
    load(resources_tuple[1])
    return [*resources_tuple[0], *resources_tuple[1]]


def transform_odk_data_to_extracted_variables(
    odk_extracted_resources: list[ExtractedResource],
    odk_raw_data: list[ODKData],
    variable_mapping: VariableMapping,
) -> list[ExtractedVariable]:
    """Transform odk variables to mex variables.

    Args:
        odk_extracted_resources: extracted mex resources
        odk_raw_data: raw data extracted from Excel files
        variable_mapping: variable mapping default values

    Returns:
        list of mex variables
    """
    extracted_variables = []
    resource_id_by_identifier_in_primary_source = {
        resource.identifierInPrimarySource: resource.stableTargetId
        for resource in odk_extracted_resources
    }
    for file in odk_raw_data:
        file_name = file.file_name.split(".xlsx")[0]
        used_in = resource_id_by_identifier_in_primary_source[file_name]
        for row_index, type_row in enumerate(file.type_survey):
            if is_invalid_odk_variable(type_row):
                continue
            # only the first rule (forValues without setValues) drops the data type
            if (
                variable_mapping.dataType[0].mappingRules[0].forValues
                and type_row in variable_mapping.dataType[0].mappingRules[0].forValues
            ):
                data_type = None
            elif "select_" in str(type_row):
                data_type = "integer"
            else:
                data_type = str(type_row)
            name = str(file.name_survey[row_index])
            if name == "nan":
                continue
            identifier_in_primary_source = f"{name}_{file_name}"
            description = [
                str(label_column[row_index])
                for label_column in file.label_survey.values()
                if str(label_column[row_index]) != "nan"
            ]
            label = name
            value_set = get_value_set(str(file.type_survey[row_index]), file)
            extracted_variables.append(
                ExtractedVariable(
                    dataType=data_type,
                    hadPrimarySource=get_extracted_primary_source_id_by_name("odk"),
                    identifierInPrimarySource=identifier_in_primary_source,
                    description=description,
                    label=label,
                    usedIn=used_in,
                    valueSet=value_set,
                )
            )
    return extracted_variables


def get_value_set(type_cell: str, file: ODKData) -> list[str]:
    """Get value sets for types cells that start with select_one or multiple_one.

    Args:
        type_cell: one type cell
        file: choice sheet corresponding to type cell

    Returns:
        list of value sets matched to type cell
    """
    value_set_survey = (
        type_cell.removeprefix("select_one").removeprefix("select_multiple").strip()
    )
    label_choices = file.label_choices
    list_name = file.list_name_choices
    names = file.name_choices
    value_set_choices = []
    for i, list_name_row in enumerate(list_name):
        if list_name_row == value_set_survey:
            for label_column in label_choices.values():
                label_value = str(label_column[i])
                name = str(names[i])
                if label_value == "nan":
                    value_set_choices.append(name)
                else:
                    value_set_choices.append(f"{name}, {label_value}")

    return value_set_choices

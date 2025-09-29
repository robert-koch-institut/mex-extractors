from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.odk.filter import is_invalid_odk_variable
from mex.extractors.odk.model import ODKData
from mex.extractors.sinks import load


def transform_odk_resources_to_mex_resources(  # noqa: PLR0913
    odk_resource_mappings: list[ResourceMapping],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    external_partner_and_publisher_by_label: dict[str, MergedOrganizationIdentifier],
    extracted_international_projects_activities: list[ExtractedActivity],
    extracted_primary_source_mex: ExtractedPrimarySource,
    extracted_primary_source_odk: ExtractedPrimarySource,
) -> tuple[list[ExtractedResource], list[ExtractedResource]]:
    """Transform odk resources to mex resources.

    Args:
        odk_resource_mappings: list of resource mapping models
        unit_stable_target_ids_by_synonym: dict of OrganizationalUnitIds
        extracted_primary_source_international_projects: primary source
        external_partner_and_publisher_by_label: dict of wikidata OrganizationIDs
        extracted_international_projects_activities: list of extracted international
                                                     projects activities
        extracted_primary_source_mex: mex primary source
        extracted_primary_source_odk: odk primary source

    Returns:
        tuple of list of mex child and non-child resources
    """
    international_projects_stable_target_id_by_identifier_in_primary_source = {
        activity.identifierInPrimarySource: activity.stableTargetId
        for activity in extracted_international_projects_activities
    }
    resources_tuple: tuple[list[ExtractedResource], list[ExtractedResource]] = ([], [])
    for resource in odk_resource_mappings:
        alternative_title = None
        if rules := resource.alternativeTitle:
            alternative_title = rules[0].mappingRules[0].setValues
        contributing_unit = None
        if resource.contributingUnit:
            contributing_unit = [
                unit_stable_target_ids_by_synonym[synonym]
                for synonym in (
                    resource.contributingUnit[0].mappingRules[0].forValues or []
                )
            ]
        description = None
        if resource.description:
            description = resource.description[0].mappingRules[0].setValues
        has_legal_basis = (
            resource.hasLegalBasis[0].mappingRules[0].setValues
            if resource.hasLegalBasis
            else []
        )
        identifier_in_primary_source = (
            resource.identifierInPrimarySource[0].mappingRules[0].setValues
        )
        method_description = None
        if resource.methodDescription:
            method_description = resource.methodDescription[0].mappingRules[0].setValues
        size_of_data_basis = None
        if resource.sizeOfDataBasis:
            size_of_data_basis = resource.sizeOfDataBasis[0].mappingRules[0].setValues
        was_generated_by = (
            international_projects_stable_target_id_by_identifier_in_primary_source[
                resource.wasGeneratedBy[0].mappingRules[0].forValues[0]  # type: ignore[index]
            ]
        )
        external_partner: list[MergedOrganizationIdentifier] = []
        for partner in resource.externalPartner[0].mappingRules[0].forValues or []:
            if partner in external_partner_and_publisher_by_label:
                external_partner.append(
                    external_partner_and_publisher_by_label[partner]
                )
            else:
                organization = ExtractedOrganization(
                    identifierInPrimarySource=partner,
                    officialName=partner,
                    hadPrimarySource=extracted_primary_source_mex.stableTargetId,
                )
                load([organization])
                external_partner.append(organization.stableTargetId)
        publisher = [
            partner
            for name in (resource.publisher[0].mappingRules[0].forValues or [])
            if (partner := external_partner_and_publisher_by_label.get(name))  # type: ignore[assignment]
        ]
        resources_tuple[bool(resource.isPartOf)].append(
            ExtractedResource(
                identifierInPrimarySource=identifier_in_primary_source,
                accessRestriction=resource.accessRestriction[0]
                .mappingRules[0]
                .setValues,
                alternativeTitle=alternative_title,
                contact=unit_stable_target_ids_by_synonym[
                    resource.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
                ],
                contributingUnit=contributing_unit,
                description=description,
                externalPartner=external_partner,
                hadPrimarySource=extracted_primary_source_odk.stableTargetId,
                hasLegalBasis=has_legal_basis,
                keyword=resource.keyword[0].mappingRules[0].setValues,
                language=resource.language[0].mappingRules[0].setValues,
                meshId=resource.meshId[0].mappingRules[0].setValues,
                method=resource.method[0].mappingRules[0].setValues,
                methodDescription=method_description,
                publisher=publisher,
                resourceCreationMethod=resource.resourceCreationMethod[0]
                .mappingRules[0]
                .setValues,
                resourceTypeGeneral=resource.resourceTypeGeneral[0]
                .mappingRules[0]
                .setValues,
                resourceTypeSpecific=resource.resourceTypeSpecific[0]
                .mappingRules[0]
                .setValues,
                rights=resource.rights[0].mappingRules[0].setValues,
                sizeOfDataBasis=size_of_data_basis,
                spatial=resource.spatial[0].mappingRules[0].setValues,
                temporal=resource.temporal[0].mappingRules[0].setValues,
                theme=resource.theme[0].mappingRules[0].setValues,
                title=resource.title[0].mappingRules[0].setValues,
                unitInCharge=unit_stable_target_ids_by_synonym[
                    resource.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
                ],
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
        resource.isPartOf = [main_questionnaire_id]
    load(resources_tuple[1])
    return [*resources_tuple[0], *resources_tuple[1]]


def transform_odk_data_to_extracted_variables(
    extracted_resources_odk: list[ExtractedResource],
    odk_raw_data: list[ODKData],
    extracted_primary_source_odk: ExtractedPrimarySource,
    variable_mapping: VariableMapping,
) -> list[ExtractedVariable]:
    """Transform odk variables to mex variables.

    Args:
        extracted_resources_odk: extracted mex resources
        odk_raw_data: raw data extracted from Excel files
        extracted_primary_source_odk: odk primary source
        variable_mapping: variable mapping default values

    Returns:
        list of mex variables
    """
    extracted_variables = []
    resource_id_by_identifier_in_primary_source = {
        resource.identifierInPrimarySource: resource.stableTargetId
        for resource in extracted_resources_odk
    }
    for file in odk_raw_data:
        file_name = file.file_name.split(".xlsx")[0]
        used_in = resource_id_by_identifier_in_primary_source[file_name]
        value_set: list[str] = []
        for row_index, type_row in enumerate(file.type_survey):
            if is_invalid_odk_variable(type_row):
                continue
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
                    hadPrimarySource=extracted_primary_source_odk.stableTargetId,
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

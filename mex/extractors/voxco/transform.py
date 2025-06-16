from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.voxco.model import VoxcoVariable


def transform_voxco_resource_mappings_to_extracted_resources(  # noqa: PLR0912, PLR0913
    voxco_resource_mappings: list[ResourceMapping],
    organization_stable_target_id_by_query_voxco: dict[
        str, MergedOrganizationIdentifier
    ],
    extracted_mex_persons_voxco: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_source_voxco: ExtractedPrimarySource,
    extracted_international_projects_activities: list[ExtractedActivity],
) -> dict[str, ExtractedResource]:
    """Transform voxco resource mappings to extracted resources.

    Args:
        voxco_resource_mappings: voxco resource mapping models
        organization_stable_target_id_by_query_voxco: extracted voxco organizations dict
        extracted_mex_persons_voxco: extracted voxco mex persons
        unit_stable_target_ids_by_synonym: merged organizational units by name
        extracted_organization_rki: extracted rki organization
        extracted_primary_source_voxco: extracted voxco primary source
        extracted_international_projects_activities: list of international projects

    Returns:
        dict extracted voxco resource by identifier in primary source
    """
    resource_dict = {}
    mex_persons_stable_target_id_by_email = {
        person.email[0]: person.stableTargetId for person in extracted_mex_persons_voxco
    }
    international_project_by_identifier_in_primary_source = {
        activity.identifierInPrimarySource: activity.stableTargetId
        for activity in extracted_international_projects_activities
    }
    for resource in voxco_resource_mappings:
        access_restriction = resource.accessRestriction[0].mappingRules[0].setValues
        if resource.anonymizationPseudonymization:
            anonymization_pseudonymization = (
                resource.anonymizationPseudonymization[0].mappingRules[0].setValues
            )
        else:
            anonymization_pseudonymization = None
        if at := resource.alternativeTitle:
            alternative_title = at[0].mappingRules[0].setValues
        else:
            alternative_title = None
        if contact_values := resource.contact[0].mappingRules[0].forValues:
            contact = mex_persons_stable_target_id_by_email[contact_values[1]]  # type: ignore[index]
        else:
            contact = None
        if description_top_level := resource.description:
            description = description_top_level[0].mappingRules[0].setValues
        else:
            description = None

        if (ep := resource.externalPartner) and ep[0].mappingRules[0].forValues:
            external_partner = organization_stable_target_id_by_query_voxco.get(
                ep[0].mappingRules[0].forValues[0]
            )
        else:
            external_partner = None
        identifier_in_primary_source: str = (
            resource.identifierInPrimarySource[0].mappingRules[0].setValues  # type: ignore[assignment]
        )
        if keyword_top_level := resource.keyword:
            keyword = keyword_top_level[0].mappingRules[0].setValues
        else:
            keyword = None
        language = resource.language[0].mappingRules[0].setValues
        mesh_id = resource.meshId[0].mappingRules[0].setValues
        method = resource.method[0].mappingRules[0].setValues
        publisher = extracted_organization_rki.stableTargetId

        resource_creation_method = (
            resource.resourceCreationMethod[0].mappingRules[0].setValues
        )
        resource_type_general = (
            resource.resourceTypeGeneral[0].mappingRules[0].setValues
        )
        resource_type_specific = (
            resource.resourceTypeSpecific[0].mappingRules[0].setValues
        )
        quality_information = resource.qualityInformation[0].mappingRules[0].setValues
        rights = resource.rights[0].mappingRules[0].setValues
        spatial = resource.spatial[0].mappingRules[0].setValues
        theme = resource.theme[0].mappingRules[0].setValues
        title = resource.title[0].mappingRules[0].setValues
        unit_in_charge = unit_stable_target_ids_by_synonym[
            resource.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ]
        if wgb := resource.wasGeneratedBy:
            was_generated_by = (
                international_project_by_identifier_in_primary_source.get(
                    wgb[0].mappingRules[0].forValues[0]  # type: ignore[index]
                )
            )
        else:
            was_generated_by = None
        resource_dict[identifier_in_primary_source] = ExtractedResource(
            accessRestriction=access_restriction,
            anonymizationPseudonymization=anonymization_pseudonymization,
            alternativeTitle=alternative_title,
            contact=contact,
            description=description,
            externalPartner=external_partner,
            hadPrimarySource=extracted_primary_source_voxco.stableTargetId,
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=keyword,
            language=language,
            meshId=mesh_id,
            method=method,
            publisher=publisher,
            resourceCreationMethod=resource_creation_method,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource_type_specific,
            qualityInformation=quality_information,
            rights=rights,
            spatial=spatial,
            theme=theme,
            title=title,
            unitInCharge=unit_in_charge,
            wasGeneratedBy=was_generated_by,
        )
    return resource_dict


def transform_voxco_variable_mappings_to_extracted_variables(
    extracted_voxco_resources: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
    extracted_primary_source_voxco: ExtractedPrimarySource,
) -> list[ExtractedVariable]:
    """Transform voxco variable mappings to extracted variables.

    Args:
        extracted_voxco_resources: extracted voxco resources
        voxco_variables: list of voxco variables by associated resource
        extracted_primary_source_voxco: extracted voxco primary source

    Returns:
        list of extracted variables
    """
    return [
        ExtractedVariable(
            hadPrimarySource=extracted_primary_source_voxco.stableTargetId,
            identifierInPrimarySource=str(variable.Id),
            dataType=variable.DataType,
            description=variable.Type,
            label=variable.QuestionText if variable.QuestionText else variable.Text,
            usedIn=resource.stableTargetId,
            valueSet=[
                choice.split("Text=")[1].split(";")[0] for choice in variable.Choices
            ],
        )
        for resource in extracted_voxco_resources.values()
        for variable in voxco_variables[f"project_{resource.identifierInPrimarySource}"]
    ]

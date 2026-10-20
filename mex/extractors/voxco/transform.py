from typing import TYPE_CHECKING

from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ResourceMapping,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.types import MergedOrganizationIdentifier, MergedPersonIdentifier
    from mex.extractors.voxco.model import VoxcoVariable


def transform_voxco_resource_mappings_to_extracted_resources(
    voxco_resource_mappings: list[ResourceMapping],
    voxco_merged_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    voxco_extracted_persons: dict[str, MergedPersonIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    international_projects_extracted_activities: list[ExtractedActivity],
) -> dict[str, ExtractedResource]:
    """Transform voxco resource mappings to extracted resources.

    Args:
        voxco_resource_mappings: voxco resource mapping models
        voxco_merged_organization_ids_by_query_string: extracted voxco organizations
                                                       dict
        voxco_extracted_persons: extracted voxco mex persons
        extracted_organization_rki: extracted rki organization
        voxco_extracted_primary_source: extracted voxco primary source
        international_projects_extracted_activities: list of international projects

    Returns:
        dict extracted voxco resource by identifier in primary source
    """
    resource_dict = {}

    international_project_by_identifier_in_primary_source = {
        activity.identifierInPrimarySource: activity.stableTargetId
        for activity in international_projects_extracted_activities
    }
    for resource in voxco_resource_mappings:
        if contact_values := resource.contact.get_mappable_values():
            contact = voxco_extracted_persons[contact_values[1]]
        else:
            contact = None

        if external_partners := resource.externalPartner.get_mappable_values():
            external_partner = voxco_merged_organization_ids_by_query_string.get(
                external_partners[0]
            )
        else:
            external_partner = None
        identifier_in_primary_source: str = (
            resource.identifierInPrimarySource.get_static_values()  # type: ignore[assignment]
        )
        publisher = extracted_organization_rki.stableTargetId

        unit_in_charge = get_unit_merged_id_by_synonym(
            resource.unitInCharge.get_mappable_values()[0]
        )
        if was_generated_by_ids := resource.wasGeneratedBy.get_mappable_values():
            was_generated_by = (
                international_project_by_identifier_in_primary_source.get(
                    was_generated_by_ids[0]
                )
            )
        else:
            was_generated_by = None

        resource_dict[identifier_in_primary_source] = ExtractedResource(
            accessRestriction=resource.accessRestriction.get_static_values(),
            anonymizationPseudonymization=resource.anonymizationPseudonymization.get_static_values(),
            alternativeTitle=resource.alternativeTitle.get_static_values(),
            contact=contact,
            description=resource.description.get_static_values(),
            externalPartner=external_partner,
            hadPrimarySource=get_extracted_primary_source_id_by_name("voxco"),
            healthCategory=resource.healthCategory.get_static_values(),
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=resource.keyword.get_static_values(),
            language=resource.language.get_static_values(),
            meshId=resource.meshId.get_static_values(),
            method=resource.method.get_static_values(),
            publisher=publisher,
            resourceCreationMethod=resource.resourceCreationMethod.get_static_values(),
            resourceTypeGeneral=resource.resourceTypeGeneral.get_static_values(),
            resourceTypeSpecific=resource.resourceTypeSpecific.get_static_values(),
            qualityInformation=resource.qualityInformation.get_static_values(),
            rights=resource.rights.get_static_values(),
            spatial=resource.spatial.get_static_values(),
            theme=resource.theme.get_static_values(),
            title=resource.title.get_static_values(),
            unitInCharge=unit_in_charge,
            wasGeneratedBy=was_generated_by,
        )
    return resource_dict


def transform_voxco_variable_mappings_to_extracted_variables(
    voxco_extracted_resources_by_str: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
) -> list[ExtractedVariable]:
    """Transform voxco variable mappings to extracted variables.

    Args:
        voxco_extracted_resources_by_str: extracted voxco resources
        voxco_variables: list of voxco variables by associated resource

    Returns:
        list of extracted variables
    """
    return [
        ExtractedVariable(
            hadPrimarySource=get_extracted_primary_source_id_by_name("voxco"),
            identifierInPrimarySource=str(variable.Id),
            dataType=variable.DataType,
            description=variable.Type,
            label=variable.QuestionText or variable.Text,
            usedIn=resource.stableTargetId,
            valueSet=[
                choice.split("Text=")[1].split(";")[0] for choice in variable.Choices
            ],
        )
        for resource in voxco_extracted_resources_by_str.values()
        for variable in voxco_variables[f"project_{resource.identifierInPrimarySource}"]
    ]

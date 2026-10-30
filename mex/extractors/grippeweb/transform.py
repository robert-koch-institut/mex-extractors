from typing import TYPE_CHECKING, Any

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.utils import model_copy_validated

if TYPE_CHECKING:
    from mex.common.types import (
        MergedContactPointIdentifier,
        MergedOrganizationIdentifier,
        MergedPersonIdentifier,
    )


def transform_grippeweb_resource_mappings_to_extracted_resources(
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    grippeweb_extracted_persons: dict[str, MergedPersonIdentifier],
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
) -> tuple[ExtractedResource, ExtractedResource]:
    """Transform grippe web values to extracted resources and link them.

    Args:
        grippeweb_resource_mappings: grippeweb resource mapping models
        grippeweb_extracted_access_platform: extracted grippeweb access platform
        grippeweb_extracted_persons: extracted grippeweb mex persons
        grippeweb_merged_organization_ids_by_query_str:
            extracted grippeweb organizations dict
        grippeweb_merged_contact_point_id_by_email:
            extracted grippeweb mex functional accounts

    Returns:
        tuple of grippeweb resources
    """
    parent_resource, child_resource = transform_grippeweb_resource_mappings_to_dict(
        grippeweb_resource_mappings,
        grippeweb_extracted_access_platform,
        grippeweb_extracted_persons,
        grippeweb_merged_organization_ids_by_query_str,
        grippeweb_merged_contact_point_id_by_email,
    )
    return parent_resource, model_copy_validated(
        child_resource, {"relatedResource": [parent_resource.stableTargetId]}
    )


def transform_grippeweb_resource_mappings_to_dict(
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    grippeweb_extracted_persons: dict[str, MergedPersonIdentifier],
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
) -> tuple[ExtractedResource, ExtractedResource]:
    """Transform grippe web values to extracted resources.

    Args:
        grippeweb_resource_mappings: grippeweb resource mapping models
        grippeweb_extracted_access_platform: extracted grippeweb access platform
        grippeweb_extracted_persons: extracted grippeweb mex persons
        grippeweb_merged_organization_ids_by_query_str:
            extracted grippeweb organizations dict
        grippeweb_merged_contact_point_id_by_email:
            extracted grippeweb mex functional accounts

    Returns:
        tuple of grippeweb resources
    """
    resource_dict = {}
    for resource in grippeweb_resource_mappings:
        contact = grippeweb_merged_contact_point_id_by_email[
            resource.contact.get_mappable_values()[0]
        ]
        contributing_unit = get_unit_merged_id_by_synonym(
            resource.contributingUnit.get_mappable_values()[0]
        )
        contributor = [
            person_id
            for name in resource.contributor.get_mappable_values()
            if (person_id := grippeweb_extracted_persons.get(name))
        ]
        external_partner_identifier = get_or_create_external_partner(
            resource,
            grippeweb_merged_organization_ids_by_query_str,
        )

        identifier_in_primary_source = (
            resource.identifierInPrimarySource.get_static_values()
            or resource.identifierInPrimarySource.get_mappable_values()[0]
        )

        publisher = grippeweb_merged_organization_ids_by_query_str.get(
            resource.publisher.get_mappable_values()[0]
        )

        unit_in_charge = get_unit_merged_id_by_synonym(
            resource.unitInCharge.get_mappable_values()[0]
        )
        resource_dict[identifier_in_primary_source] = ExtractedResource(
            accessPlatform=grippeweb_extracted_access_platform.stableTargetId,
            accessRestriction=resource.accessRestriction.get_static_values(),
            accrualPeriodicity=resource.accrualPeriodicity.get_static_values(),
            anonymizationPseudonymization=resource.anonymizationPseudonymization.get_static_values(),
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            description=resource.description.get_static_values(),
            documentation=resource.documentation.get_static_values(),
            externalPartner=external_partner_identifier,
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            hasCodeValues=resource.hasCodeValues.get_static_values(),
            hasLegalBasis=resource.hasLegalBasis.get_static_values(),
            hasPersonalData=resource.hasPersonalData.get_static_values(),
            hasPurposeDescription=resource.hasPurposeDescription.get_static_values(),
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=resource.keyword.get_static_values(),
            language=resource.language.get_static_values(),
            meshId=resource.meshId.get_static_values(),
            method=resource.method.get_static_values(),
            methodDescription=resource.methodDescription.get_static_values(),
            minTypicalAge=resource.minTypicalAge.get_static_values(),
            populationCoverage=resource.populationCoverage.get_static_values(),
            publisher=publisher,
            resourceCreationMethod=resource.resourceCreationMethod.get_static_values(),
            resourceTypeGeneral=resource.resourceTypeGeneral.get_static_values(),
            resourceTypeSpecific=resource.resourceTypeSpecific.get_static_values(),
            rights=resource.rights.get_static_values(),
            spatial=resource.spatial.get_static_values(),
            start=resource.start.get_static_values(),
            stateOfDataProcessing=resource.stateOfDataProcessing.get_static_values(),
            theme=resource.theme.get_static_values(),
            title=resource.title.get_static_values(),
            unitInCharge=unit_in_charge,
        )
    return resource_dict["grippeweb"], resource_dict["grippeweb-plus"]


def get_or_create_external_partner(
    resource: ResourceMapping,
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
) -> list[MergedOrganizationIdentifier] | None:
    """Get external partner from wikidata or create organization.

    Args:
        resource: grippeweb resource mapping model
        grippeweb_merged_organization_ids_by_query_str:
            extracted grippeweb organizations dict

    Returns:
        dict extracted grippeweb resource by identifier in primary source
    """
    if external_partners := resource.externalPartner.get_mappable_values():
        external_partner_string = external_partners[0]

        if external_partner_string in resource.model_fields:
            external_partner_identifier = [
                grippeweb_merged_organization_ids_by_query_str[external_partner_string]
            ]
        else:
            external_partner_organization = ExtractedOrganization(
                officialName=external_partner_string,
                identifierInPrimarySource=external_partner_string,
                hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            )
            load([external_partner_organization])
            external_partner_identifier = [external_partner_organization.stableTargetId]
        return external_partner_identifier
    return None


def transform_grippeweb_access_platform_to_extracted_access_platform(
    grippeweb_access_platform: AccessPlatformMapping,
    grippeweb_extracted_persons: dict[str, MergedPersonIdentifier],
) -> ExtractedAccessPlatform:
    """Transform grippeweb access platform to ExtractedAccessPlatform.

    Args:
        grippeweb_access_platform: grippeweb access platform mapping model
        grippeweb_extracted_persons: extracted grippeweb persons

    Returns:
        ExtractedAccessPlatform grippeweb
    """
    contact = [
        grippeweb_extracted_persons[email]
        for email in grippeweb_access_platform.contact.get_mappable_values()
    ]

    unit_in_charge = get_unit_merged_id_by_synonym(
        grippeweb_access_platform.unitInCharge.get_mappable_values()[0]
    )

    return ExtractedAccessPlatform(
        contact=contact,
        hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
        identifierInPrimarySource=grippeweb_access_platform.identifierInPrimarySource.get_static_values(),
        technicalAccessibility=grippeweb_access_platform.technicalAccessibility.get_static_values(),
        title=grippeweb_access_platform.title.get_static_values(),
        unitInCharge=unit_in_charge,
    )


def transform_grippeweb_variable_group_to_extracted_variable_groups(
    grippeweb_variable_group: VariableGroupMapping,
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform Grippeweb variable groups to extracted variable groups.

    Args:
        grippeweb_variable_group: grippeweb variable group mapping model
        grippeweb_columns: grippeweb data by column and table
        grippeweb_extracted_parent_resource: extracted parent resource

    Returns:
        list of extracted variable groups
    """
    return [
        ExtractedVariableGroup(
            containedBy=grippeweb_extracted_parent_resource.stableTargetId,
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            identifierInPrimarySource=table_name,
            label=grippeweb_variable_group.label.get_mapped_values(table_name),
        )
        for table_name in grippeweb_columns
    ]


def transform_grippeweb_variable_to_extracted_variables(
    grippeweb_variable: VariableMapping,
    grippeweb_extracted_variables_groups: list[ExtractedVariableGroup],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform Grippeweb variables to extracted variables.

    Args:
        grippeweb_variable: grippeweb variable mapping model
        grippeweb_extracted_variables_groups: extracted grippeweb variable groups
        grippeweb_columns: grippeweb data by column and table
        grippeweb_extracted_parent_resource: extracted parent resource

    Returns:
        list of extracted variables
    """
    valueset_locations_by_field = {
        valueset.fieldInPrimarySource: valueset.locationInPrimarySource
        for valueset in grippeweb_variable.valueSet
    }
    variable_group_by_location = {
        group.identifierInPrimarySource: group.stableTargetId
        for group in grippeweb_extracted_variables_groups
    }
    extracted_variables: list[ExtractedVariable] = []
    seen_column_names: list[str] = []
    for table_name, table in grippeweb_columns.items():
        for column_name, column in table.items():
            if column_name in seen_column_names:
                continue
            seen_column_names.append(column_name)
            if (
                valueset_locations_by_field.get(column_name)
                == "vMasterDataMEx AND vWeeklyResponsesMEx"
            ):
                belongs_to = [
                    variable_group_by_location["vMasterDataMEx"],
                    variable_group_by_location["vWeeklyResponsesMEx"],
                ]
                value_set = list(
                    {
                        k: None
                        for k in (
                            grippeweb_columns["vMasterDataMEx"][column_name]
                            + grippeweb_columns["vWeeklyResponsesMEx"][column_name]
                        )
                        if k is not None
                    }
                )
            else:
                belongs_to = [variable_group_by_location[table_name]]
                column_strings = list(
                    {cell: None for cell in column if isinstance(cell, str)}
                )
                value_set = (
                    column_strings if column_name in valueset_locations_by_field else []
                )
            extracted_variables.append(
                ExtractedVariable(
                    belongsTo=belongs_to,
                    hadPrimarySource=get_extracted_primary_source_id_by_name(
                        "grippeweb"
                    ),
                    identifierInPrimarySource=column_name,
                    label=column_name,
                    usedIn=grippeweb_extracted_parent_resource.stableTargetId,
                    valueSet=value_set,
                )
            )
    return extracted_variables

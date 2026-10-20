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
    child_resource.relatedResource = [parent_resource.stableTargetId]
    return parent_resource, child_resource


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
        access_restriction = resource.accessRestriction.get_static_values()
        accrual_periodicity = resource.accrualPeriodicity.get_static_values()
        anonymization_pseudonymization = (
            resource.anonymizationPseudonymization.get_static_values()
        )
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
        description = resource.description.get_static_values()
        documentation = resource.documentation.get_static_values()
        external_partner_identifier = get_or_create_external_partner(
            resource,
            grippeweb_merged_organization_ids_by_query_str,
        )

        has_legal_basis = resource.hasLegalBasis.get_static_values()
        has_personal_data = resource.hasPersonalData.get_static_values()
        has_purpose_description = resource.hasPurposeDescription.get_static_values()
        has_code_values = resource.hasCodeValues.get_static_values()
        identifier_in_primary_source = (
            resource.identifierInPrimarySource.get_static_values()
            or resource.identifierInPrimarySource.get_mappable_values()[0]
        )

        keyword = resource.keyword.get_static_values()
        language = resource.language.get_static_values()
        mesh_id = resource.meshId.get_static_values()
        method = resource.method.get_static_values()
        method_description = resource.methodDescription.get_static_values()
        min_typical_age = resource.minTypicalAge.get_static_values()
        population_coverage = resource.populationCoverage.get_static_values()
        publisher = grippeweb_merged_organization_ids_by_query_str.get(
            resource.publisher.get_mappable_values()[0]
        )

        resource_creation_method = resource.resourceCreationMethod.get_static_values()
        resource_type_general = resource.resourceTypeGeneral.get_static_values()
        resource_type_specific = resource.resourceTypeSpecific.get_static_values()
        rights = resource.rights.get_static_values()
        spatial = resource.spatial.get_static_values()
        start = resource.start.get_static_values()
        state_of_data_processing = resource.stateOfDataProcessing.get_static_values()
        theme = resource.theme.get_static_values()
        title = resource.title.get_static_values()
        unit_in_charge = get_unit_merged_id_by_synonym(
            resource.unitInCharge.get_mappable_values()[0]
        )
        resource_dict[identifier_in_primary_source] = ExtractedResource(
            accessPlatform=grippeweb_extracted_access_platform.stableTargetId,
            accessRestriction=access_restriction,
            accrualPeriodicity=accrual_periodicity,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            description=description,
            documentation=documentation,
            externalPartner=external_partner_identifier,
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            hasCodeValues=has_code_values,
            hasLegalBasis=has_legal_basis,
            hasPersonalData=has_personal_data,
            hasPurposeDescription=has_purpose_description,
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=keyword,
            language=language,
            meshId=mesh_id,
            method=method,
            methodDescription=method_description,
            minTypicalAge=min_typical_age,
            populationCoverage=population_coverage,
            publisher=publisher,
            resourceCreationMethod=resource_creation_method,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource_type_specific,
            rights=rights,
            spatial=spatial,
            start=start,
            stateOfDataProcessing=state_of_data_processing,
            theme=theme,
            title=title,
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
    identifier_in_primary_source = (
        grippeweb_access_platform.identifierInPrimarySource.get_static_values()
    )

    contact = [
        grippeweb_extracted_persons[email]
        for email in grippeweb_access_platform.contact.get_mappable_values()
    ]

    technical_accessibility = (
        grippeweb_access_platform.technicalAccessibility.get_static_values()
    )
    title = grippeweb_access_platform.title.get_static_values()

    unit_in_charge = get_unit_merged_id_by_synonym(
        grippeweb_access_platform.unitInCharge.get_mappable_values()[0]
    )

    return ExtractedAccessPlatform(
        contact=contact,
        hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
        identifierInPrimarySource=identifier_in_primary_source,
        technicalAccessibility=technical_accessibility,
        title=title,
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

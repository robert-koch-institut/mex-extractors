from typing import Any

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load


def transform_grippeweb_resource_mappings_to_extracted_resources(  # noqa: PLR0913
    grippeweb_resource_mappings: list[ResourceMapping],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    grippeweb_extracted_persons: list[ExtractedPerson],
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
) -> tuple[ExtractedResource, ExtractedResource]:
    """Transform grippe web values to extracted resources and link them.

    Args:
        grippeweb_resource_mappings: grippeweb resource mapping models
        unit_stable_target_ids_by_synonym: merged organizational units by name
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
        unit_stable_target_ids_by_synonym,
        grippeweb_extracted_access_platform,
        grippeweb_extracted_persons,
        grippeweb_merged_organization_ids_by_query_str,
        grippeweb_merged_contact_point_id_by_email,
    )
    child_resource.isPartOf = [parent_resource.stableTargetId]
    return parent_resource, child_resource


def transform_grippeweb_resource_mappings_to_dict(  # noqa: PLR0913
    grippeweb_resource_mappings: list[ResourceMapping],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    grippeweb_extracted_persons: list[ExtractedPerson],
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
) -> tuple[ExtractedResource, ExtractedResource]:
    """Transform grippe web values to extracted resources.

    Args:
        grippeweb_resource_mappings: grippeweb resource mapping models
        unit_stable_target_ids_by_synonym: merged organizational units by name
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
    mex_persons_by_name = {
        person.fullName[0]: person for person in grippeweb_extracted_persons
    }
    for resource in grippeweb_resource_mappings:
        access_restriction = resource.accessRestriction[0].mappingRules[0].setValues
        accrual_periodicity = resource.accrualPeriodicity[0].mappingRules[0].setValues
        anonymization_pseudonymization = (
            resource.anonymizationPseudonymization[0].mappingRules[0].setValues
        )
        contact = grippeweb_merged_contact_point_id_by_email[
            resource.contact[0].mappingRules[0].forValues[0].lower()  # type: ignore[index]
        ]
        contributing_unit = unit_stable_target_ids_by_synonym[
            resource.contributingUnit[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ]
        contributor = [
            person.stableTargetId
            for name in (resource.contributor[0].mappingRules[0].forValues or [])
            if (
                person := mex_persons_by_name.get(
                    f"{name.split(' ')[1]}, {name.split(' ')[0]}"
                )
            )
        ]
        created = resource.created[0].mappingRules[0].setValues
        description = resource.description[0].mappingRules[0].setValues
        documentation = resource.documentation[0].mappingRules[0].setValues
        external_partner_identifier = get_or_create_external_partner(
            resource,
            grippeweb_merged_organization_ids_by_query_str,
        )

        has_legal_basis = resource.hasLegalBasis[0].mappingRules[0].setValues
        has_personal_data = resource.hasPersonalData[0].mappingRules[0].setValues
        icd10code = resource.icd10code[0].mappingRules[0].setValues
        identifier_in_primary_source_mapping_rules = resource.identifierInPrimarySource[
            0
        ].mappingRules[0]
        if set_values := identifier_in_primary_source_mapping_rules.setValues:
            identifier_in_primary_source = set_values
        else:
            identifier_in_primary_source = (
                identifier_in_primary_source_mapping_rules.forValues[0]  # type: ignore[index]
            )
        keyword = resource.keyword[0].mappingRules[0].setValues
        language = resource.language[0].mappingRules[0].setValues
        mesh_id = resource.meshId[0].mappingRules[0].setValues
        method = resource.method[0].mappingRules[0].setValues
        method_description = resource.methodDescription[0].mappingRules[0].setValues
        min_typical_age = resource.minTypicalAge[0].mappingRules[0].setValues
        population_coverage = resource.populationCoverage[0].mappingRules[0].setValues
        publisher = grippeweb_merged_organization_ids_by_query_str.get(
            resource.publisher[0].mappingRules[0].forValues[0]  # type: ignore[index]
        )

        resource_creation_method = (
            resource.resourceCreationMethod[0].mappingRules[0].setValues
        )
        resource_type_general = (
            resource.resourceTypeGeneral[0].mappingRules[0].setValues
        )
        resource_type_specific = (
            resource.resourceTypeSpecific[0].mappingRules[0].setValues
        )
        rights = resource.rights[0].mappingRules[0].setValues
        size_of_data_basis = resource.sizeOfDataBasis[0].mappingRules[0].setValues
        spatial = resource.spatial[0].mappingRules[0].setValues
        state_of_data_processing = (
            resource.stateOfDataProcessing[0].mappingRules[0].setValues
        )
        temporal = resource.temporal[0].mappingRules[0].setValues
        theme = resource.theme[0].mappingRules[0].setValues
        title = resource.title[0].mappingRules[0].setValues
        unit_in_charge = unit_stable_target_ids_by_synonym[
            resource.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ]
        # wasGeneratedField was removed for one resource mapping, but kept for the other
        # only look this field up if it exists in mapping
        was_generated_by = None
        if wgb := resource.wasGeneratedBy:
            was_generated_by = unit_stable_target_ids_by_synonym[
                wgb[0].mappingRules[0].forValues[0]  # type: ignore[index]
            ]
        resource_dict[identifier_in_primary_source] = ExtractedResource(
            hasLegalBasis=has_legal_basis,
            hasPersonalData=has_personal_data,
            minTypicalAge=min_typical_age,
            populationCoverage=population_coverage,
            resourceCreationMethod=resource_creation_method,
            accessPlatform=grippeweb_extracted_access_platform.stableTargetId,
            accessRestriction=access_restriction,
            accrualPeriodicity=accrual_periodicity,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            created=created,
            description=description,
            documentation=documentation,
            externalPartner=external_partner_identifier,
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            icd10code=icd10code,
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=keyword,
            language=language,
            meshId=mesh_id,
            method=method,
            methodDescription=method_description,
            publisher=publisher,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource_type_specific,
            rights=rights,
            sizeOfDataBasis=size_of_data_basis,
            spatial=spatial,
            stateOfDataProcessing=state_of_data_processing,
            temporal=temporal,
            theme=theme,
            title=title,
            unitInCharge=unit_in_charge,
            wasGeneratedBy=was_generated_by,
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
    if external_partner_dict := resource.externalPartner:
        external_partner_string = external_partner_dict[0].mappingRules[0].forValues[0]  # type: ignore[index]
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
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_persons: list[ExtractedPerson],
) -> ExtractedAccessPlatform:
    """Transform grippeweb access platform to ExtractedAccessPlatform.

    Args:
        grippeweb_access_platform: grippeweb access platform mapping model
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        grippeweb_extracted_persons: extracted grippeweb persons

    Returns:
        ExtractedAccessPlatform grippeweb
    """
    mex_person_stable_target_id_by_email = {
        str(person.email[0]): person.stableTargetId
        for person in grippeweb_extracted_persons
    }
    identifier_in_primary_source = (
        grippeweb_access_platform.identifierInPrimarySource[0].mappingRules[0].setValues
    )

    contact = [
        mex_person_stable_target_id_by_email[email]
        for email in (
            grippeweb_access_platform.contact[0].mappingRules[0].forValues or []
        )
    ]

    technical_accessibility = (
        grippeweb_access_platform.technicalAccessibility[0].mappingRules[0].setValues
    )
    title = grippeweb_access_platform.title[0].mappingRules[0].setValues

    unit_in_charge = unit_stable_target_ids_by_synonym[
        grippeweb_access_platform.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
    ]

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
    label_by_table_name = {
        mapping_rule.forValues[0]: mapping_rule.setValues  # type: ignore[index]
        for mapping_rule in grippeweb_variable_group.label[0].mappingRules
    }
    return [
        ExtractedVariableGroup(
            containedBy=grippeweb_extracted_parent_resource.stableTargetId,
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            identifierInPrimarySource=table_name,
            label=label_by_table_name[table_name],
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

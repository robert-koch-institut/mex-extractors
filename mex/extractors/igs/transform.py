from typing import cast

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
    Text,
)
from mex.extractors.igs.model import (
    IGSEnumSchema,
    IGSInfo,
    IGSPropertiesSchema,
    IGSSchema,
)


def transform_igs_info_to_resources(  # noqa: PLR0913
    igs_info: IGSInfo,
    igs_extracted_primary_source: ExtractedPrimarySource,
    igs_resource_mapping: ResourceMapping,
    igs_extracted_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    igs_extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Transform IGS schemas to extracted resources.

    Args:
        igs_info: igs info
        igs_extracted_primary_source: extracted IGS primary source
        igs_resource_mapping: IGS resource mapping
        igs_extracted_contact_points_by_mail: extracted IGS contact points by mail
        unit_stable_target_ids_by_synonym: merged organizational units by name
        igs_extracted_access_platform: extracted access platform
        extracted_organization_rki: extracted organization RKI

    Returns:
        extracted resource by enum
    """
    contact = [
        igs_extracted_contact_points_by_mail[for_value].stableTargetId
        for rule in igs_resource_mapping.contact[0].mappingRules
        if rule.forValues
        for for_value in rule.forValues
        if isinstance(for_value, str)
    ]
    contributing_unit = (
        [unit_stable_target_ids_by_synonym[for_value] for for_value in for_values]
        if (
            for_values := igs_resource_mapping.contributingUnit[0]
            .mappingRules[0]
            .forValues
        )
        else []
    )
    title = igs_resource_mapping.title[0].mappingRules[0].setValues
    unit_in_charge = (
        unit_stable_target_ids_by_synonym[for_value[0]]
        if (for_value := igs_resource_mapping.unitInCharge[0].mappingRules[0].forValues)
        else []
    )
    keyword = [
        set_value
        for rule in igs_resource_mapping.keyword[1].mappingRules[1:]
        if rule.setValues
        for set_value in rule.setValues
    ]
    if igs_resource_mapping.keyword[0].mappingRules[0].setValues:
        keyword.extend(igs_resource_mapping.keyword[0].mappingRules[0].setValues)

    return [
        ExtractedResource(
            accessPlatform=igs_extracted_access_platform.stableTargetId,
            accessRestriction=igs_resource_mapping.accessRestriction[0]
            .mappingRules[0]
            .setValues,
            accrualPeriodicity=igs_resource_mapping.accrualPeriodicity[0]
            .mappingRules[0]
            .setValues,
            alternativeTitle=igs_resource_mapping.alternativeTitle[0]
            .mappingRules[0]
            .setValues,
            anonymizationPseudonymization=igs_resource_mapping.anonymizationPseudonymization[
                0
            ]
            .mappingRules[0]
            .setValues,
            contact=contact,
            contributingUnit=contributing_unit,
            description=igs_resource_mapping.description[0].mappingRules[0].setValues,
            documentation=igs_resource_mapping.documentation[0]
            .mappingRules[0]
            .setValues,
            hadPrimarySource=igs_extracted_primary_source.stableTargetId,
            hasLegalBasis=igs_resource_mapping.hasLegalBasis[0]
            .mappingRules[0]
            .setValues,
            hasPersonalData=igs_resource_mapping.hasPersonalData[0]
            .mappingRules[0]
            .setValues,
            identifierInPrimarySource=f"IGS_{igs_info.title}_v{igs_info.version}",
            keyword=keyword,
            language=igs_resource_mapping.language[0].mappingRules[0].setValues,
            meshId=igs_resource_mapping.meshId[0].mappingRules[0].setValues,
            method=igs_resource_mapping.method[0].mappingRules[0].setValues,
            publisher=extracted_organization_rki.stableTargetId,
            resourceCreationMethod=igs_resource_mapping.resourceCreationMethod[0]
            .mappingRules[0]
            .setValues,
            resourceTypeGeneral=igs_resource_mapping.resourceTypeGeneral[0]
            .mappingRules[0]
            .setValues,
            resourceTypeSpecific=igs_resource_mapping.resourceTypeSpecific[0]
            .mappingRules[0]
            .setValues,
            rights=igs_resource_mapping.rights[0].mappingRules[0].setValues,
            spatial=igs_resource_mapping.spatial[0].mappingRules[0].setValues,
            temporal=igs_resource_mapping.temporal[0].mappingRules[0].setValues,
            theme=igs_resource_mapping.theme[0].mappingRules[0].setValues,
            title=title,
            unitInCharge=unit_in_charge,
        )
    ]


def transform_igs_access_platform(
    igs_extracted_primary_source: ExtractedPrimarySource,
    igs_access_platform_mapping: AccessPlatformMapping,
    igs_extracted_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedAccessPlatform:
    """Transform IGS extracted access platform.

    Args:
        igs_extracted_primary_source: extracted IGS primary source
        igs_access_platform_mapping: IGS resource mapping
        igs_extracted_contact_points_by_mail: extracted IGS contact points by mail
        unit_stable_target_ids_by_synonym: merged organizational units by name

    Returns:
        extracted IGS access platform
    """
    contact_mail = cast(
        "list[str]", igs_access_platform_mapping.contact[0].mappingRules[0].forValues
    )
    contact = igs_extracted_contact_points_by_mail[contact_mail[0]].stableTargetId
    unit_string = cast(
        "list[str]",
        igs_access_platform_mapping.unitInCharge[0].mappingRules[0].forValues,
    )
    unit_in_charge = unit_stable_target_ids_by_synonym[unit_string[0]]
    return ExtractedAccessPlatform(
        endpointDescription=igs_access_platform_mapping.endpointDescription[0]
        .mappingRules[0]
        .setValues,
        endpointType=igs_access_platform_mapping.endpointType[0]
        .mappingRules[0]
        .setValues,
        endpointURL=igs_access_platform_mapping.endpointURL[0]
        .mappingRules[0]
        .setValues,
        contact=contact,
        description=igs_access_platform_mapping.description[0]
        .mappingRules[0]
        .setValues,
        hadPrimarySource=igs_extracted_primary_source.stableTargetId,
        identifierInPrimarySource=igs_access_platform_mapping.identifierInPrimarySource[
            0
        ]
        .mappingRules[0]
        .setValues,
        landingPage=igs_access_platform_mapping.landingPage[0]
        .mappingRules[0]
        .setValues,
        technicalAccessibility=igs_access_platform_mapping.technicalAccessibility[0]
        .mappingRules[0]
        .setValues,
        title=igs_access_platform_mapping.title[0].mappingRules[0].forValues,
        unitInCharge=unit_in_charge,
    )


def transformed_igs_schemas_to_variable_group(
    filtered_schemas: dict[str, IGSSchema],
    igs_extracted_resource_ids_by_identifier_in_primary_source: dict[
        str, MergedResourceIdentifier
    ],
    igs_extracted_primary_source: ExtractedPrimarySource,
    igs_info: IGSInfo,
) -> list[ExtractedVariableGroup]:
    """Transform IGS schema to extracted variable group.

    Args:
        filtered_schemas: filtered IGS schemas
        igs_extracted_resource_ids_by_identifier_in_primary_source:
            dict of igs resource ids
        igs_extracted_primary_source: extracted_primary source igs
        igs_info: igs info

    Returns:
        list of extracted variable groups
    """
    extracted_variable_groups: list[ExtractedVariableGroup] = []
    for schema_name in filtered_schemas:
        contained_by = igs_extracted_resource_ids_by_identifier_in_primary_source[
            f"IGS_{igs_info.title}_v{igs_info.version}"
        ]
        label = (
            [Text(value="Health Agency", language="en")]
            if "HaCreation" in schema_name
            else [Text(value=schema_name.removesuffix("Creation"), language="en")]
        )
        extracted_variable_groups.append(
            ExtractedVariableGroup(
                containedBy=contained_by,
                hadPrimarySource=igs_extracted_primary_source.stableTargetId,
                identifierInPrimarySource=schema_name,
                label=label,
            )
        )
    return extracted_variable_groups


def get_enums_by_property_name(
    igs_schemas: dict[str, IGSSchema],
) -> dict[str, list[str]]:
    """Return a dictionary that links enum lists to property_name.

    Args:
        igs_schemas: dictionary of igs schemas by schema name

    Returns:
        enum list by property name
    """
    enum_by_schema_name = {
        schema_name: schema.enum
        for schema_name, schema in igs_schemas.items()
        if isinstance(schema, IGSEnumSchema)
    }
    return {
        property_name: enum_by_schema_name[schema_name]
        for schema in igs_schemas.values()
        if isinstance(schema, IGSPropertiesSchema)
        for property_name, properties in schema.properties.items()
        if (
            "$ref" in properties
            and (schema_name := properties["$ref"].split("/")[-1])
            in enum_by_schema_name
        )
        or (
            "items" in properties
            and "$ref" in properties["items"]
            and (schema_name := properties["items"]["$ref"].split("/")[-1])
            in enum_by_schema_name
        )
    }


def transform_igs_schemas_to_variables(  # noqa: PLR0913
    igs_schemas: dict[str, IGSSchema],
    igs_extracted_resource_ids_by_identifier_in_primary_source: dict[
        str, MergedResourceIdentifier
    ],
    igs_extracted_primary_source: ExtractedPrimarySource,
    extracted_igs_variable_group_ids_by_igs_identifier: dict[
        str, MergedVariableGroupIdentifier
    ],
    variable_mapping: VariableMapping,
    variable_pathogen_mapping: VariableMapping,
    igs_info: IGSInfo,
) -> list[ExtractedVariable]:
    """Transform igs schemas to variables.

    Args:
        igs_schemas: igs schemas by schema name
        igs_extracted_resource_ids_by_identifier_in_primary_source:
            igs resources by pathogen
        igs_extracted_primary_source: extracted primary source igs
        extracted_igs_variable_group_ids_by_igs_identifier:
            igs variable group by identifier in primary source
        variable_mapping: variable mapping default values
        variable_pathogen_mapping: variable_pathogen mapping default values
        igs_info: igs info

    Returns:
        list of ExtractedVariable
    """
    description_by_enum = {
        rule.forValues[0]: rule.setValues
        for rule in variable_pathogen_mapping.description[0].mappingRules
        if rule.forValues
    }
    enums_by_property_name = get_enums_by_property_name(igs_schemas)
    extracted_variables: list[ExtractedVariable] = []
    used_in = igs_extracted_resource_ids_by_identifier_in_primary_source[
        f"IGS_{igs_info.title}_v{igs_info.version}"
    ]
    for schema_name, schema in igs_schemas.items():
        data_type: str | None = None
        if schema_name == "Pathogen" and isinstance(schema, IGSEnumSchema):
            data_type = schema.type
            for enum in schema.enum:
                belongs_to = (
                    [extracted_igs_variable_group_ids_by_igs_identifier[schema_name]]
                    if schema_name in extracted_igs_variable_group_ids_by_igs_identifier
                    else []
                )
                description = description_by_enum.get(enum)
                identifier_in_primary_source = f"pathogen_{enum}"
                label = enum
                extracted_variables.append(
                    ExtractedVariable(
                        belongsTo=belongs_to,
                        dataType=data_type,
                        description=description,
                        hadPrimarySource=igs_extracted_primary_source.stableTargetId,
                        identifierInPrimarySource=identifier_in_primary_source,
                        label=label,
                        usedIn=used_in,
                    )
                )

        if not isinstance(schema, IGSPropertiesSchema):
            continue
        for property_name, schema_property in schema.properties.items():
            belongs_to = (
                [extracted_igs_variable_group_ids_by_igs_identifier[schema_name]]
                if schema_name in extracted_igs_variable_group_ids_by_igs_identifier
                else []
            )
            if (
                schema_property
                and variable_mapping.dataType[1].mappingRules[0].forValues
                and "format" in schema_property
                and schema_property["format"]
                in variable_mapping.dataType[1].mappingRules[0].forValues
            ):
                data_type = schema_property["format"]
            elif "type" in schema_property:
                data_type = schema_property["type"]
            value_set = enums_by_property_name.get(property_name, [])
            extracted_variables.append(
                ExtractedVariable(
                    belongsTo=belongs_to,
                    dataType=data_type,
                    description=schema_property.get("title"),
                    hadPrimarySource=igs_extracted_primary_source.stableTargetId,
                    identifierInPrimarySource=property_name,
                    label=property_name,
                    valueSet=value_set,
                    usedIn=used_in,
                )
            )
    return extracted_variables

from typing import cast

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
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


def transform_igs_schemas_to_resources(  # noqa: PLR0913
    igs_schemas: dict[str, IGSSchema],
    igs_info: IGSInfo,
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_resource_mapping: ResourceMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedResource]:
    """Transform IGS schemas to extracted resources.

    Args:
        igs_schemas: IGS schema by name
        igs_info: igs info
        extracted_primary_source_igs: extracted IGS primary source
        igs_resource_mapping: IGS resource mapping
        extracted_igs_contact_points_by_mail: extracted IGS contact points by mail
        unit_stable_target_ids_by_synonym: merged organizational units by name


    Returns:
        extracted resource by enum
    """
    contact = [
        extracted_igs_contact_points_by_mail[for_value].stableTargetId
        for rule in igs_resource_mapping.contact[0].mappingRules
        if rule.forValues
        for for_value in rule.forValues
        if isinstance(for_value, str)
    ]
    title = igs_resource_mapping.title[0].mappingRules[0].setValues
    unit_in_charge = (
        unit_stable_target_ids_by_synonym[for_value[0]]
        if (for_value := igs_resource_mapping.unitInCharge[0].mappingRules[0].forValues)
        else []
    )
    pathogen_schema = cast("IGSEnumSchema", igs_schemas["Pathogen"])

    return [
        ExtractedResource(
            accessRestriction=igs_resource_mapping.accessRestriction[0]
            .mappingRules[0]
            .setValues,
            contact=contact,
            hadPrimarySource=extracted_primary_source_igs.stableTargetId,
            identifierInPrimarySource=f"IGS_{igs_info.title}_v{igs_info.version}",
            theme=igs_resource_mapping.theme[0].mappingRules[0].setValues,
            title=title,
            unitInCharge=unit_in_charge,
        )
        for pathogen in pathogen_schema.enum
    ]


def transform_igs_access_platform(
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_access_platform_mapping: AccessPlatformMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedAccessPlatform:
    """Transform IGS extracted access platform.

    Args:
        extracted_primary_source_igs: extracted IGS primary source
        igs_access_platform_mapping: IGS resource mapping
        extracted_igs_contact_points_by_mail: extracted IGS contact points by mail
        unit_stable_target_ids_by_synonym: merged organizational units by name

    Returns:
        extracted IGS access platform
    """
    contact_mail = cast(
        "list[str]", igs_access_platform_mapping.contact[0].mappingRules[0].forValues
    )
    contact = extracted_igs_contact_points_by_mail[contact_mail[0]].stableTargetId
    unit_string = cast(
        "list[str]",
        igs_access_platform_mapping.unitInCharge[0].mappingRules[0].forValues,
    )
    unit_in_charge = unit_stable_target_ids_by_synonym[unit_string[0]]
    return ExtractedAccessPlatform(
        contact=contact,
        hadPrimarySource=extracted_primary_source_igs.stableTargetId,
        identifierInPrimarySource=igs_access_platform_mapping.identifierInPrimarySource[
            0
        ]
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
    extracted_igs_resource_ids_by_identifier_in_primary_source: dict[
        str, MergedResourceIdentifier
    ],
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_info: IGSInfo,
) -> list[ExtractedVariableGroup]:
    """Transform IGS schema to extracted variable group.

    Args:
        filtered_schemas: filtered IGS schemas
        extracted_igs_resource_ids_by_identifier_in_primary_source:
            dict of igs resource ids
        extracted_primary_source_igs: extracted_primary source igs
        igs_info: igs info

    Returns:
        list of extracted variable groups
    """
    extracted_variable_groups: list[ExtractedVariableGroup] = []
    for schema_name in filtered_schemas:
        contained_by = extracted_igs_resource_ids_by_identifier_in_primary_source[
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
                hadPrimarySource=extracted_primary_source_igs.stableTargetId,
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
    extracted_igs_resource_ids_by_identifier_in_primary_source: dict[
        str, MergedResourceIdentifier
    ],
    extracted_primary_source_igs: ExtractedPrimarySource,
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
        extracted_igs_resource_ids_by_identifier_in_primary_source:
            igs resources by pathogen
        extracted_primary_source_igs: extracted primary source igs
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
    used_in = extracted_igs_resource_ids_by_identifier_in_primary_source[
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
                        hadPrimarySource=extracted_primary_source_igs.stableTargetId,
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
                    hadPrimarySource=extracted_primary_source_igs.stableTargetId,
                    identifierInPrimarySource=property_name,
                    label=property_name,
                    valueSet=value_set,
                    usedIn=used_in,
                )
            )
    return extracted_variables

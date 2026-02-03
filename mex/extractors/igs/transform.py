from typing import cast

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import (
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
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


def transform_igs_extracted_resource(  # noqa: PLR0913
    igs_resource_mapping: ResourceMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
    igs_extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
    igs_schemas: dict[str, IGSSchema],
    igs_info: IGSInfo,
    igs_endpoint_counts: dict[str, str],
) -> dict[str, ExtractedResource]:
    """Transform IGS schemas to extracted resources.

    Args:
        igs_resource_mapping: IGS resource mapping
        igs_extracted_contact_points_by_mail_str: extracted IGS contact points by mail
        igs_extracted_access_platform: extracted access platform
        extracted_organization_rki: extracted organization RKI
        igs_schemas: igs schema dictionary
        igs_info: IGS info
        igs_endpoint_counts: IGS endpoint count dictionary

    Returns:
        igs extracted resource by pathogen
    """
    pathogens = cast("IGSEnumSchema", igs_schemas["igsmodels__enums__Pathogen"]).enum
    contact = [
        igs_extracted_contact_points_by_mail_str[for_value].stableTargetId
        for rule in igs_resource_mapping.contact[0].mappingRules
        if rule.forValues
        for for_value in rule.forValues
        if isinstance(for_value, str)
    ]
    contributing_units_by_pathogen = {
        for_value: rule.rule
        for rule in igs_resource_mapping.contributingUnit[0].mappingRules[1:]
        if rule.forValues and rule.rule
        for for_value in rule.forValues
    }
    created = igs_resource_mapping.created[0].mappingRules[0].setValues
    unit_in_charge = (
        get_unit_merged_id_by_synonym(for_value[0])
        if (for_value := igs_resource_mapping.unitInCharge[0].mappingRules[0].forValues)
        else []
    )
    keywords_by_pathogen = {
        rule.forValues[0]: rule.setValues
        for rule in igs_resource_mapping.keyword[1].mappingRules
        if rule.forValues and rule.setValues
    }
    quality_information_values_by_field_in_primary_source = {
        field.fieldInPrimarySource: field.mappingRules[0]
        .setValues[0]
        .value.split("[")[0]
        for field in igs_resource_mapping.qualityInformation
        if field.mappingRules[0].setValues
    }
    title_by_pathogen = {
        rule.forValues[0]: rule.setValues
        for rule in igs_resource_mapping.title[0].mappingRules
        if rule.forValues and rule.setValues
    }
    extracted_resources_by_pathogen: dict[str, ExtractedResource] = {}
    for pathogen in pathogens:
        contributing_units = [
            unit
            for synonym in contributing_units_by_pathogen[pathogen].split(",")
            if (units := get_unit_merged_id_by_synonym(synonym))
            for unit in units
        ]
        identifier_in_primary_source = f"{igs_info.title}_{pathogen}"
        keyword = igs_resource_mapping.keyword[0].mappingRules[0].setValues
        if keyword:
            keyword.append(Text(value=pathogen.removesuffix("P")))
            keyword.extend(keywords_by_pathogen[pathogen])
        quality_information = [
            Text(
                value=f"{quality_information_values_by_field_in_primary_source[key]}{value}",
                language="de",
            )
            for key, value in igs_endpoint_counts.items()
            if "pathogen" not in key and "upload" not in key
        ]
        quality_information.append(
            Text(
                value=f"Anzahl Genomsequenzen: {igs_endpoint_counts[f'pathogen_{pathogen}']}"  # noqa: E501
            )
        )
        if igs_resource_mapping.sizeOfDataBasis[0].fieldInPrimarySource:
            size_of_databasis = igs_endpoint_counts[
                igs_resource_mapping.sizeOfDataBasis[0].fieldInPrimarySource
            ]
        extracted_resources_by_pathogen[pathogen] = ExtractedResource(
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
            contact=contact,
            contributingUnit=contributing_units,
            created=created,
            description=igs_resource_mapping.description[0].mappingRules[0].setValues,
            documentation=igs_resource_mapping.documentation[0]
            .mappingRules[0]
            .setValues,
            hadPrimarySource=get_extracted_primary_source_id_by_name("igs"),
            hasLegalBasis=igs_resource_mapping.hasLegalBasis[0]
            .mappingRules[0]
            .setValues,
            hasPurpose=igs_resource_mapping.hasPurpose[0].mappingRules[0].setValues,
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=keyword,
            language=igs_resource_mapping.language[0].mappingRules[0].setValues,
            meshId=igs_resource_mapping.meshId[0].mappingRules[0].setValues,
            method=igs_resource_mapping.method[0].mappingRules[0].setValues,
            populationCoverage=igs_resource_mapping.populationCoverage[0]
            .mappingRules[0]
            .setValues,
            provenance=igs_resource_mapping.provenance[0].mappingRules[0].setValues,
            publisher=extracted_organization_rki.stableTargetId,
            qualityInformation=quality_information,
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
            sizeOfDataBasis=size_of_databasis,
            theme=igs_resource_mapping.theme[0].mappingRules[0].setValues,
            title=title_by_pathogen[pathogen],
            unitInCharge=unit_in_charge,
        )
    return extracted_resources_by_pathogen


def transform_igs_access_platform(
    igs_access_platform_mapping: AccessPlatformMapping,
    igs_extracted_contact_points_by_mail_str: dict[str, ExtractedContactPoint],
) -> ExtractedAccessPlatform:
    """Transform IGS extracted access platform.

    Args:
        igs_access_platform_mapping: IGS resource mapping
        igs_extracted_contact_points_by_mail_str: extracted IGS contact points by mail

    Returns:
        extracted IGS access platform
    """
    contact_mail = cast(
        "list[str]", igs_access_platform_mapping.contact[0].mappingRules[0].forValues
    )
    contact = igs_extracted_contact_points_by_mail_str[contact_mail[0]].stableTargetId
    unit_string = cast(
        "list[str]",
        igs_access_platform_mapping.unitInCharge[0].mappingRules[0].forValues,
    )
    unit_in_charge = get_unit_merged_id_by_synonym(unit_string[0])
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
        hadPrimarySource=get_extracted_primary_source_id_by_name("igs"),
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
        title=igs_access_platform_mapping.title[0].mappingRules[0].setValues,
        unitInCharge=unit_in_charge,
    )


def transformed_igs_schemas_to_variable_group(
    filtered_schemas: dict[str, IGSSchema],
    igs_extracted_resource_id: MergedResourceIdentifier,
) -> list[ExtractedVariableGroup]:
    """Transform IGS schema to extracted variable group.

    Args:
        filtered_schemas: filtered IGS schemas
        igs_extracted_resource_id: stableTargetId of the igs resource


    Returns:
        list of extracted variable groups
    """
    extracted_variable_groups: list[ExtractedVariableGroup] = []
    for schema_name in filtered_schemas:
        contained_by = igs_extracted_resource_id
        label = (
            [Text(value="Health Agency", language="en")]
            if "HaCreation" in schema_name
            else [Text(value=schema_name.removesuffix("Creation"), language="en")]
        )
        extracted_variable_groups.append(
            ExtractedVariableGroup(
                containedBy=contained_by,
                hadPrimarySource=get_extracted_primary_source_id_by_name("igs"),
                identifierInPrimarySource=schema_name,
                label=label,
            )
        )
    return extracted_variable_groups


def get_enums_by_property_name(  # TODO(EH): deprecated, remove in MX-2095
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
        or (
            "anyOf" in properties
            and "$ref" in properties["anyOf"][0]
            and (schema_name := properties["anyOf"][0]["$ref"].split("/")[-1])
            in enum_by_schema_name
        )
    }


def transform_igs_schemas_to_variables(
    igs_schemas: dict[str, IGSSchema],
    igs_extracted_resource_id: MergedResourceIdentifier,
    extracted_igs_variable_group_ids_by_igs_identifier: dict[
        str, MergedVariableGroupIdentifier
    ],
    variable_mapping: VariableMapping,
    variable_pathogen_mapping: VariableMapping,
) -> list[ExtractedVariable]:
    """Transform igs schemas to variables.

    Args:
        igs_schemas: igs schemas by schema name
        igs_extracted_resource_id: stableTargetId of the igs resource
        extracted_igs_variable_group_ids_by_igs_identifier:
            igs variable group by identifier in primary source
        variable_mapping: variable mapping default values
        variable_pathogen_mapping: variable_pathogen mapping default values

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
    used_in = igs_extracted_resource_id
    for schema_name, schema in igs_schemas.items():
        data_type: str | None = None
        if schema_name == "igsmodels__enums__Pathogen" and isinstance(
            schema, IGSEnumSchema
        ):
            data_type = schema.type
            for enum in schema.enum:
                if schema_name in extracted_igs_variable_group_ids_by_igs_identifier:
                    belongs_to = [
                        extracted_igs_variable_group_ids_by_igs_identifier[schema_name]
                    ]
                else:
                    continue
                description = description_by_enum.get(enum)
                identifier_in_primary_source = f"pathogen_{enum}"
                label = enum
                extracted_variables.append(
                    ExtractedVariable(
                        belongsTo=belongs_to,
                        dataType=data_type,
                        description=description,
                        hadPrimarySource=get_extracted_primary_source_id_by_name("igs"),
                        identifierInPrimarySource=identifier_in_primary_source,
                        label=label,
                        usedIn=used_in,
                    )
                )

        if not isinstance(schema, IGSPropertiesSchema):
            continue
        for property_name, schema_property in schema.properties.items():
            if schema_name in extracted_igs_variable_group_ids_by_igs_identifier:
                belongs_to = [
                    extracted_igs_variable_group_ids_by_igs_identifier[schema_name]
                ]
            else:
                continue
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
                    hadPrimarySource=get_extracted_primary_source_id_by_name("igs"),
                    identifierInPrimarySource=f"{schema_name}_{property_name}",
                    label=property_name,
                    valueSet=value_set,
                    usedIn=used_in,
                )
            )
    return extracted_variables

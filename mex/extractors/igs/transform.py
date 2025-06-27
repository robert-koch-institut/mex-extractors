from typing import cast

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import MergedOrganizationalUnitIdentifier, Text, TextLanguage
from mex.extractors.igs.model import IGSEnumSchema, IGSSchema


def transform_igs_schemas_to_resources(
    igs_schemas: dict[str, IGSSchema],
    extracted_primary_source_igs: ExtractedPrimarySource,
    igs_resource_mapping: ResourceMapping,
    extracted_igs_contact_points_by_mail: dict[str, ExtractedContactPoint],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> dict[str, ExtractedResource]:
    """Transform IGS schemas to extracted resources.

    Args:
        igs_schemas: IGS schema by name
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
    title_by_pathogen = {
        rule.forValues[0]: rule.setValues
        for rule in igs_resource_mapping.title[0].mappingRules
        if rule.setValues and rule.forValues
    }
    unit_in_charge = (
        unit_stable_target_ids_by_synonym[for_value[0]]
        if (for_value := igs_resource_mapping.unitInCharge[0].mappingRules[0].forValues)
        else []
    )
    pathogen_schema = cast("IGSEnumSchema", igs_schemas["Pathogen"])

    return {
        pathogen: ExtractedResource(
            accessRestriction=igs_resource_mapping.accessRestriction[0]
            .mappingRules[0]
            .setValues,
            contact=contact,
            hadPrimarySource=extracted_primary_source_igs.stableTargetId,
            identifierInPrimarySource=f"IGS_{pathogen}",
            theme=igs_resource_mapping.theme[0].mappingRules[0].setValues,
            title=title_by_pathogen.get(
                pathogen,
                Text(
                    value=f"Integrierte Genomische Surveillance {pathogen}",
                    language=TextLanguage.DE,
                ),
            ),
            unitInCharge=unit_in_charge,
        )
        for pathogen in pathogen_schema.enum
    }


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

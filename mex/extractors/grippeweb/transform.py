from typing import Any

from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.common.types import (
    Email,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)


def transform_grippeweb_resource_mappings_to_extracted_resources(
    grippeweb_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
) -> dict[str, ExtractedResource]:
    """Transform grippe web values to extracted resources and link them.

    Args:
        grippeweb_resource_mappings: grippeweb  resource mappings
        unit_stable_target_ids_by_synonym: merged organizational units by name
        grippeweb_extracted_access_platform: extracted grippeweb access platform
        extracted_primary_source_grippeweb: extracted grippeweb primary source
        extracted_mex_persons_grippeweb: extracted grippeweb mex persons
        grippeweb_organization_ids_by_query_string:
            extracted grippeweb organizations dict
        extracted_mex_functional_units_grippeweb:
            extracted grippeweb mex functional accounts
        extracted_confluence_vvt_activities:
            extracted confluence vvt activities

    Returns:
        grippeweb resources by identifierInPrimarySource
    """
    resource_dict = transform_grippeweb_resource_mappings_to_dict(
        grippeweb_resource_mappings,
        unit_stable_target_ids_by_synonym,
        grippeweb_extracted_access_platform,
        extracted_primary_source_grippeweb,
        extracted_mex_persons_grippeweb,
        grippeweb_organization_ids_by_query_string,
        extracted_mex_functional_units_grippeweb,
    )
    resource_dict["grippeweb-plus"].isPartOf = [
        resource_dict["grippeweb"].stableTargetId
    ]
    return resource_dict


def transform_grippeweb_resource_mappings_to_dict(
    grippeweb_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
) -> dict[str, ExtractedResource]:
    """Transform grippe web values to extracted resources.

    Args:
        grippeweb_resource_mappings: grippeweb  resource mappings
        unit_stable_target_ids_by_synonym: merged organizational units by name
        grippeweb_extracted_access_platform: extracted grippeweb access platform
        extracted_primary_source_grippeweb: extracted grippeweb primary source
        extracted_mex_persons_grippeweb: extracted grippeweb mex persons
        grippeweb_organization_ids_by_query_string:
            extracted grippeweb organizations dict
        extracted_mex_functional_units_grippeweb:
            extracted grippeweb mex functional accounts

    Returns:
        dict extracted grippeweb resource by identifier in primary source
    """
    resource_dict = {}
    mex_persons_by_name = {
        person.fullName[0]: person for person in extracted_mex_persons_grippeweb
    }
    for resource in grippeweb_resource_mappings:
        access_restriction = resource["accessRestriction"][0]["mappingRules"][0][
            "setValues"
        ]
        accrual_periodicity = resource["accrualPeriodicity"][0]["mappingRules"][0][
            "setValues"
        ]
        anonymization_pseudonymization = resource["anonymizationPseudonymization"][0][
            "mappingRules"
        ][0]["setValues"]
        contact = extracted_mex_functional_units_grippeweb[
            resource["contact"][0]["mappingRules"][0]["forValues"][0].lower()
        ]
        contributing_unit = unit_stable_target_ids_by_synonym[
            resource["contributingUnit"][0]["mappingRules"][0]["forValues"][0]
        ]
        contributor = [
            mex_persons_by_name[
                f"{name.split(' ')[1]}, {name.split(' ')[0]}"
            ].stableTargetId
            for name in resource["contributor"][0]["mappingRules"][0]["forValues"]
        ]
        created = resource["created"][0]["mappingRules"][0]["setValues"]
        description = resource["description"][0]["mappingRules"][0]["setValues"]
        documentation = resource["documentation"][0]["mappingRules"][0]["setValues"]
        has_legal_basis = resource["hasLegalBasis"][0]["mappingRules"][0]["setValues"]
        has_personal_data = resource["hasPersonalData"][0]["mappingRules"][0][
            "setValues"
        ]
        icd10code = resource["icd10code"][0]["mappingRules"][0]["setValues"]
        identifier_in_primary_source_mapping_rules = resource[
            "identifierInPrimarySource"
        ][0]["mappingRules"][0]
        if set_values := identifier_in_primary_source_mapping_rules["setValues"]:
            identifier_in_primary_source = set_values[0]
        else:
            identifier_in_primary_source = identifier_in_primary_source_mapping_rules[
                "forValues"
            ][0]
        keyword = resource["keyword"][0]["mappingRules"][0]["setValues"]
        language = resource["language"][0]["mappingRules"][0]["setValues"]
        mesh_id = resource["meshId"][0]["mappingRules"][0]["setValues"]
        method = resource["method"][0]["mappingRules"][0]["setValues"]
        method_description = resource["methodDescription"][0]["mappingRules"][0][
            "setValues"
        ]
        min_typical_age = resource["minTypicalAge"][0]["mappingRules"][0]["setValues"]
        population_coverage = resource["populationCoverage"][0]["mappingRules"][0][
            "setValues"
        ]
        publisher = grippeweb_organization_ids_by_query_string.get(
            resource["publisher"][0]["mappingRules"][0]["forValues"][0]
        )

        resource_creation_method = resource["resourceCreationMethod"][0][
            "mappingRules"
        ][0]["setValues"]
        resource_type_general = resource["resourceTypeGeneral"][0]["mappingRules"][0][
            "setValues"
        ]
        resource_type_specific = resource["resourceTypeSpecific"][0]["mappingRules"][0][
            "setValues"
        ]
        rights = resource["rights"][0]["mappingRules"][0]["setValues"]
        size_of_data_basis = resource["sizeOfDataBasis"][0]["mappingRules"][0][
            "setValues"
        ]
        spatial = resource["spatial"][0]["mappingRules"][0]["setValues"]
        state_of_data_processing = resource["stateOfDataProcessing"][0]["mappingRules"][
            0
        ]["setValues"]
        temporal = resource["temporal"][0]["mappingRules"][0]["setValues"]
        theme = resource["theme"][0]["mappingRules"][0]["setValues"]
        title = resource["title"][0]["mappingRules"][0]["setValues"]
        unit_in_charge = unit_stable_target_ids_by_synonym[
            resource["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
        ]
        # wasGeneratedField was removed for one resource mapping, but kept for the other
        # only look this field up if it exists in mapping
        was_generated_by = None
        if wgb := resource.get("wasGeneratedBy"):
            was_generated_by = unit_stable_target_ids_by_synonym[
                wgb[0]["mappingRules"][0]["forValues"][0]
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
            hadPrimarySource=extracted_primary_source_grippeweb.stableTargetId,
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
    return resource_dict


def transform_grippeweb_access_platform_to_extracted_access_platform(
    grippeweb_access_platform: dict[str, Any],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
) -> ExtractedAccessPlatform:
    """Transform grippeweb access platform to ExtractedAccessPlatform.

    Args:
        grippeweb_access_platform: grippeweb extracted access platform
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_primary_source: Extracted primary source
        extracted_mex_persons_grippeweb: extracted grippeweb persons

    Returns:
        ExtractedAccessPlatform grippeweb
    """
    mex_person_stable_target_id_by_email = {
        person.email[0]: person.stableTargetId
        for person in extracted_mex_persons_grippeweb
    }
    identifier_in_primary_source = grippeweb_access_platform[
        "identifierInPrimarySource"
    ][0]["mappingRules"][0]["setValues"]

    contact = [
        mex_person_stable_target_id_by_email[email]
        for email in grippeweb_access_platform["contact"][0]["mappingRules"][0][
            "forValues"
        ]
    ]

    technical_accessibility = grippeweb_access_platform["technicalAccessibility"][0][
        "mappingRules"
    ][0]["setValues"]
    title = grippeweb_access_platform["title"][0]["mappingRules"][0]["setValues"]

    unit_in_charge = unit_stable_target_ids_by_synonym[
        grippeweb_access_platform["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
    ]

    return ExtractedAccessPlatform(
        contact=contact,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=identifier_in_primary_source,
        technicalAccessibility=technical_accessibility,
        title=title,
        unitInCharge=unit_in_charge,
    )


def transform_grippeweb_variable_group_to_extracted_variable_groups(
    grippeweb_variable_group: dict[str, Any],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_resource_dict: dict[str, ExtractedResource],
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
) -> list[ExtractedVariableGroup]:
    """Transform Grippeweb variable groups to extracted variable groups.

    Args:
        grippeweb_variable_group: grippeweb variable group default values
        grippeweb_columns: grippeweb data by column and table
        grippeweb_extracted_resource_dict: extracted resources by name
        extracted_primary_source_grippeweb: Extracted primary source

    Returns:
        list of extracted variable groups
    """
    label_by_table_name = {
        mapping_rules["forValues"][0]: mapping_rules["setValues"][0]
        for mapping_rules in grippeweb_variable_group["label"][0]["mappingRules"]
    }
    return [
        ExtractedVariableGroup(
            containedBy=grippeweb_extracted_resource_dict["grippeweb"].stableTargetId,
            hadPrimarySource=extracted_primary_source_grippeweb.stableTargetId,
            identifierInPrimarySource=table_name,
            label=label_by_table_name[f"MEx.{table_name}"],
        )
        for table_name in grippeweb_columns.keys()
    ]


def transform_grippeweb_variable_to_extracted_variables(
    grippeweb_variable: dict[str, Any],
    grippeweb_extracted_variable_group: list[ExtractedVariableGroup],
    grippeweb_columns: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_resource_dict: dict[str, ExtractedResource],
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
) -> list[ExtractedVariable]:
    """Transform Grippeweb variables to extracted variables.

    Args:
        grippeweb_variable: grippeweb variable default values
        grippeweb_extracted_variable_group: extracted grippeweb variable groups
        grippeweb_columns: grippeweb data by column and table
        grippeweb_extracted_resource_dict: extracted resources by name
        extracted_primary_source_grippeweb: Extracted primary source

    Returns:
        list of extracted variables
    """
    valueset_locations_by_field = {
        valueset["fieldInPrimarySource"]: valueset["locationInPrimarySource"]
        for valueset in grippeweb_variable["valueSet"]
    }
    variable_group_by_location = {
        group.identifierInPrimarySource: group.stableTargetId
        for group in grippeweb_extracted_variable_group
    }
    return [
        ExtractedVariable(
            belongsTo=(
                [
                    variable_group_by_location["vMasterDataMEx"],
                    variable_group_by_location["vWeeklyResponsesMEx"],
                ]
                if location == "vMasterDataMEx AND vWeeklyResponsesMEx"
                else [variable_group_by_location[location]]
            ),
            hadPrimarySource=extracted_primary_source_grippeweb.stableTargetId,
            identifierInPrimarySource=field,
            label=field,
            usedIn=grippeweb_extracted_resource_dict["grippeweb"].stableTargetId,
            valueSet=list(
                (
                    set(grippeweb_columns["vMasterDataMEx"][field]).union(
                        set(grippeweb_columns["vWeeklyResponsesMEx"][field])
                    )
                    if location == "vMasterDataMEx AND vWeeklyResponsesMEx"
                    else set(grippeweb_columns[location][field])
                )
                - {None}
            ),
        )
        for field, location in valueset_locations_by_field.items()
    ]

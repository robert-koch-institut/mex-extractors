from typing import Any

from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
)
from mex.common.types import (
    Email,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)


def transform_grippeweb_resource_mappings_to_extracted_resources(
    grippeweb_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
    # TODO: add extracted_confluence_vvt_sources: list[ExtractedActivity],
) -> list[ExtractedResource]:
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

    Returns:
        list ExtractedResource
    """
    resource_dict = transform_grippeweb_resource_mappings_to_dict(
        grippeweb_resource_mappings,
        unit_stable_target_ids_by_synonym,
        grippeweb_extracted_access_platform,
        extracted_primary_source_grippeweb,
        extracted_mex_persons_grippeweb,
        grippeweb_organization_ids_by_query_string,
        extracted_mex_functional_units_grippeweb,
        # TODO: add extracted_confluence_vvt_sources,
    )
    resource_dict["grippeweb-plus"].isPartOf = [
        resource_dict["grippeweb"].stableTargetId
    ]
    return list(resource_dict.values())


def transform_grippeweb_resource_mappings_to_dict(
    grippeweb_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    extracted_primary_source_grippeweb: ExtractedPrimarySource,
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
    # TODO: add extracted_confluence_vvt_sources: list[ExtractedActivity],
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
    """
    TODO: add confluence_vvt_by_identifier_in_primary_source = {
         source.identifierInPrimarySource: source.stableTargetId
         for source in extracted_confluence_vvt_sources
     }"""
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
        icd10code = resource["icd10code"][0]["mappingRules"][0]["setValues"]
        identifier_in_primary_source = resource["identifierInPrimarySource"][0][
            "mappingRules"
        ][0]["setValues"][0]
        keyword = resource["keyword"][0]["mappingRules"][0]["setValues"]
        language = resource["language"][0]["mappingRules"][0]["setValues"]
        mesh_id = resource["meshId"][0]["mappingRules"][0]["setValues"]
        method = resource["method"][0]["mappingRules"][0]["setValues"]
        method_description = resource["methodDescription"][0]["mappingRules"][0][
            "setValues"
        ]
        publication = resource["publication"][0]["mappingRules"][0]["setValues"]
        publisher = grippeweb_organization_ids_by_query_string.get(
            resource["publisher"][0]["mappingRules"][0]["forValues"][0]
        )

        resource_type_general = resource["resourceTypeGeneral"][0]["mappingRules"][0][
            "setValues"
        ]
        resource_type_specific = resource["resourceTypeSpecific"][0]["mappingRules"][0][
            "setValues"
        ]
        rights = resource["rights"][0]["mappingRules"][0]["setValues"]
        state_of_data_processing = resource["stateOfDataProcessing"][0]["mappingRules"][
            0
        ]["setValues"]
        temporal = resource["temporal"][0]["mappingRules"][0]["setValues"]
        theme = resource["theme"][0]["mappingRules"][0]["setValues"]
        title = resource["title"][0]["mappingRules"][0]["setValues"]
        unit_in_charge = unit_stable_target_ids_by_synonym[
            resource["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
        ]
        resource_dict[identifier_in_primary_source] = ExtractedResource(
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
            publication=publication,
            publisher=publisher,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource_type_specific,
            rights=rights,
            temporal=temporal,
            stateOfDataProcessing=state_of_data_processing,
            theme=theme,
            title=title,
            unitInCharge=unit_in_charge,
        )
    return resource_dict


def transform_grippeweb_access_platform_to_extracted_access_platform(
    grippeweb_access_platform: dict[str, Any],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
) -> ExtractedAccessPlatform:
    """Transform grippeweb access platform to ExtractedAccessPlatform.

    Args:
        grippeweb_access_platform: grippeweb extracted access platform
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_primary_source: Extracted primary source
        extracted_mex_functional_units_grippeweb: extracted grippeweb functional units

    Returns:
        ExtractedAccessPlatform
    """
    identifier_in_primary_source = grippeweb_access_platform[
        "identifierInPrimarySource"
    ][0]["mappingRules"][0]["setValues"]

    contact = [
        extracted_mex_functional_units_grippeweb[email.lower()]
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


def transform_wikidata_organizations_to_extracted_organizations_with_query(
    wikidata_organizations_by_query: dict[str, WikidataOrganization],
    extracted_primary_source_wikidata: ExtractedPrimarySource,
) -> dict[str, ExtractedOrganization]:
    """Return a mapping from the search query to the Extracted Organizations.

    Args:
        wikidata_organizations_by_query: dictionary with string keys and
            WikidataOrganization values
        extracted_primary_source_wikidata: ExtractedPrimarySource for Wikidata
    Returns:
        Dict with keys: search query and values: Extracted Organization.
    """
    query_to_organization = {}
    for query, organization in wikidata_organizations_by_query.items():
        if extracted_organizations := list(
            transform_wikidata_organizations_to_extracted_organizations(
                [organization], extracted_primary_source_wikidata
            )
        ):
            query_to_organization[query] = extracted_organizations[0]
        else:
            continue
    return query_to_organization
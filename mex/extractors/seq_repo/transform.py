from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
)
from mex.extractors.seq_repo.model import SeqRepoSource


def transform_seq_repo_activities_to_extracted_activities(  # noqa: PLR0913
    seq_repo_sources: dict[str, SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    seq_repo_project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
    extracted_primary_source: ExtractedPrimarySource,
) -> list[ExtractedActivity]:
    """Transform seq-repo activities to list of unique ExtractedActivity.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        seq_repo_activity: Seq Repo activity mapping models with default values
        seq_repo_source_resolved_project_coordinators: Seq Repo sources resolved project
                                            coordinators ldap query results
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        seq_repo_project_coordinators_merged_ids_by_query_string: Seq Repo Sources
                                                        resolved project coordinators
                                                        merged ids
        extracted_primary_source: Extracted primary source

    Returns:
        list of unique ExtractedActivity
    """
    theme = seq_repo_activity.theme[0].mappingRules[0].setValues
    unique_activities = []

    for source in seq_repo_sources.values():
        project_coordinators_ids, responsible_units = (
            get_resolved_project_coordinators_and_units(
                source.project_coordinators,
                seq_repo_source_resolved_project_coordinators,
                unit_stable_target_ids_by_synonym,
                seq_repo_project_coordinators_merged_ids_by_query_string,
            )
        )

        if not responsible_units or not project_coordinators_ids:
            continue

        extracted_activity = ExtractedActivity(
            contact=project_coordinators_ids,
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=source.project_id,
            involvedPerson=project_coordinators_ids,
            responsibleUnit=responsible_units,
            theme=theme,
            title=source.project_name,
        )

        if extracted_activity not in unique_activities:
            unique_activities.append(extracted_activity)

    return unique_activities


def transform_seq_repo_resource_to_extracted_resource(  # noqa: PLR0913
    seq_repo_sources: dict[str, SeqRepoSource],
    seq_repo_activities: dict[str, ExtractedActivity],
    mex_access_platform: ExtractedAccessPlatform,
    seq_repo_resource: ResourceMapping,
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    seq_repo_project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_source: ExtractedPrimarySource,
) -> list[ExtractedResource]:
    """Transform seq-repo resources to ExtractedResource.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        seq_repo_activities: Seq Repo extracted activity for default values from mapping
        mex_access_platform: Extracted access platform
        seq_repo_resource: Seq Repo resource mapping model with default values
        seq_repo_source_resolved_project_coordinators: Seq Repo sources resolved project
                                                       coordinators ldap query results
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        seq_repo_project_coordinators_merged_ids_by_query_string: Seq Repo Sources
                                                                  resolved project
                                                                  coordinators merged
                                                                  ids
        extracted_organization_rki: wikidata extracted organization
        extracted_primary_source: Extracted primary source

    Returns:
        list of ExtractedResource
    """
    # Resource values from mapping
    access_restriction = (
        seq_repo_resource.accessRestriction[0].mappingRules[0].setValues
    )
    accrual_periodicity = (
        seq_repo_resource.accrualPeriodicity[0].mappingRules[0].setValues
    )
    anonymization_pseudonymization = (
        seq_repo_resource.anonymizationPseudonymization[0].mappingRules[0].setValues
    )
    description = seq_repo_resource.description[0].mappingRules[0].setValues
    method = seq_repo_resource.method[0].mappingRules[0].setValues

    resource_creation_method = (
        seq_repo_resource.resourceCreationMethod[0].mappingRules[0].setValues
    )
    resource_type_general = (
        seq_repo_resource.resourceTypeGeneral[0].mappingRules[0].setValues
    )
    resource_type_specific = (
        seq_repo_resource.resourceTypeSpecific[0].mappingRules[0].setValues
    )
    rights = seq_repo_resource.rights[0].mappingRules[0].setValues
    state_of_data_processing = (
        seq_repo_resource.stateOfDataProcessing[0].mappingRules[0].setValues
    )
    theme = seq_repo_resource.theme[0].mappingRules[0].setValues
    shared_keyword = seq_repo_resource.keyword[0].mappingRules[0].setValues or []

    extracted_resources = []
    for identifier_in_primary_source, source in seq_repo_sources.items():
        activity = seq_repo_activities.get(source.project_id)

        project_coordinators_ids, units_in_charge = (
            get_resolved_project_coordinators_and_units(
                source.project_coordinators,
                seq_repo_source_resolved_project_coordinators,
                unit_stable_target_ids_by_synonym,
                seq_repo_project_coordinators_merged_ids_by_query_string,
            )
        )

        if not units_in_charge or not project_coordinators_ids:
            continue

        contributing_unit = unit_stable_target_ids_by_synonym.get(
            source.customer_org_unit_id
        )
        keyword = [*shared_keyword, Text(value=source.species)]
        extracted_resource = ExtractedResource(
            accessPlatform=mex_access_platform.stableTargetId,
            accessRestriction=access_restriction,
            accrualPeriodicity=accrual_periodicity,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=project_coordinators_ids,
            contributingUnit=contributing_unit or [],
            created=source.sequencing_date,
            description=description,
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=identifier_in_primary_source,
            instrumentToolOrApparatus=source.sequencing_platform,
            keyword=keyword,
            method=method,
            publisher=extracted_organization_rki.stableTargetId,
            resourceCreationMethod=resource_creation_method,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource_type_specific,
            rights=rights,
            stateOfDataProcessing=state_of_data_processing,
            theme=theme,
            title=f"{source.project_name} sample {source.customer_sample_name}",
            unitInCharge=units_in_charge,
            wasGeneratedBy=activity.stableTargetId if activity else None,
        )
        extracted_resources.append(extracted_resource)

    return extracted_resources


def transform_seq_repo_access_platform_to_extracted_access_platform(
    seq_repo_access_platform: AccessPlatformMapping,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
) -> ExtractedAccessPlatform:
    """Transform seq-repo access platform to ExtractedAccessPlatform.

    Args:
        seq_repo_access_platform: Seq Repo access platform mapping model
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_primary_source: Extracted primary source

    Returns:
        ExtractedAccessPlatform
    """
    alternative_title = (
        seq_repo_access_platform.alternativeTitle[0].mappingRules[0].setValues
    )

    description = seq_repo_access_platform.description[0].mappingRules[0].setValues
    endpoint_type = seq_repo_access_platform.endpointType[0].mappingRules[0].setValues
    identifier_in_primary_source = (
        seq_repo_access_platform.identifierInPrimarySource[0].mappingRules[0].setValues
    )
    landing_page = seq_repo_access_platform.landingPage[0].mappingRules[0].setValues

    technical_accessibility = (
        seq_repo_access_platform.technicalAccessibility[0].mappingRules[0].setValues
    )
    title = seq_repo_access_platform.title[0].mappingRules[0].setValues

    contacts = seq_repo_access_platform.contact[0].mappingRules[0].forValues or []

    resolved_organigram = [
        unit_stable_target_ids_by_synonym.get(contact) for contact in contacts
    ]

    return ExtractedAccessPlatform(
        alternativeTitle=alternative_title,
        contact=resolved_organigram,
        description=description,
        endpointType=endpoint_type,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=identifier_in_primary_source,
        landingPage=landing_page,
        technicalAccessibility=technical_accessibility,
        title=title,
        unitInCharge=resolved_organigram,
    )


def get_resolved_project_coordinators_and_units(
    project_coordinators: list[str],
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    seq_repo_project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
) -> tuple[list[MergedPersonIdentifier], list[MergedOrganizationalUnitIdentifier]]:
    """Get ldap resolved ids of project coordinators and units.

    Args:
        project_coordinators: Seq Repo raw project coordinator names
        seq_repo_source_resolved_project_coordinators: Seq Repo sources resolved project
                                            coordinators ldap query results
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        seq_repo_project_coordinators_merged_ids_by_query_string: Seq Repo Sources
                                                                  resolved project
                                                                  coordinators merged
                                                                  ids

    Returns:
        Resolved ids project coordinator and units
    """
    project_coordinators_ids = []
    units_in_charge = []
    for pc in project_coordinators:
        person_merged_id = seq_repo_project_coordinators_merged_ids_by_query_string.get(
            pc
        )
        if person_merged_id:
            project_coordinators_ids.append(person_merged_id[0])

        for query in seq_repo_source_resolved_project_coordinators:
            query_ldap: LDAPPersonWithQuery = query
            if (sam_account_name := query_ldap.person.sAMAccountName) and (  # noqa: SIM102
                department_number := query_ldap.person.departmentNumber
            ):
                if sam_account_name.lower() == pc.lower():
                    unit = unit_stable_target_ids_by_synonym.get(department_number)
                    if unit and unit not in units_in_charge:
                        units_in_charge.append(unit)
    return project_coordinators_ids, units_in_charge

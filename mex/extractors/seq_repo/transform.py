from typing import TYPE_CHECKING

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
)
from mex.extractors.ldap.helpers import get_ldap_extracted_person_by_query
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.seq_repo.model import SeqRepoSource


def transform_seq_repo_activities_to_extracted_activities(
    seq_repo_sources: dict[str, SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_merged_person_ids_by_query_string: dict[str, MergedPersonIdentifier],
) -> list[ExtractedActivity]:
    """Transform seq-repo activities to list of unique ExtractedActivity.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        seq_repo_activity: Seq Repo activity mapping models with default values
        seq_repo_merged_person_ids_by_query_string: Seq Repo sources resolved project
                                            coordinators ldap query results

    Returns:
        list of unique ExtractedActivity
    """
    theme = seq_repo_activity.theme[0].mappingRules[0].setValues
    unique_activities = []

    for source in seq_repo_sources.values():
        responsible_units = get_resolved_project_coordinator_units(
            source.project_coordinators,
        )

        if not responsible_units:
            continue

        extracted_activity = ExtractedActivity(
            contact=list(seq_repo_merged_person_ids_by_query_string.values()),
            hadPrimarySource=get_extracted_primary_source_id_by_name("seq-repo"),
            identifierInPrimarySource=source.project_id,
            involvedPerson=list(seq_repo_merged_person_ids_by_query_string.values()),
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
    seq_repo_merged_person_ids_by_query_string: dict[str, MergedPersonIdentifier],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Transform seq-repo resources to ExtractedResource.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        seq_repo_activities: Seq Repo extracted activity for default values from mapping
        mex_access_platform: Extracted access platform
        seq_repo_resource: Seq Repo resource mapping model with default values
        seq_repo_merged_person_ids_by_query_string: Seq Repo Sources
                                                                  resolved project
                                                                  coordinators merged
                                                                  ids
        extracted_organization_rki: wikidata extracted organization

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

        units_in_charge = get_resolved_project_coordinator_units(
            source.project_coordinators,
        )

        if not units_in_charge:
            continue
        contributing_unit = get_unit_merged_id_by_synonym(source.customer_org_unit_id)
        keyword = shared_keyword
        if source.species:
            keyword.append(Text(value=source.species))
        extracted_resource = ExtractedResource(
            accessPlatform=mex_access_platform.stableTargetId,
            accessRestriction=access_restriction,
            accrualPeriodicity=accrual_periodicity,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=list(seq_repo_merged_person_ids_by_query_string.values()),
            contributingUnit=contributing_unit,
            created=source.sequencing_date,
            description=description,
            hadPrimarySource=get_extracted_primary_source_id_by_name("seq-repo"),
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
) -> ExtractedAccessPlatform:
    """Transform seq-repo access platform to ExtractedAccessPlatform.

    Args:
        seq_repo_access_platform: Seq Repo access platform mapping model

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
        unit_id
        for contact in contacts
        if (unit_ids := get_unit_merged_id_by_synonym(contact))
        for unit_id in unit_ids
    ]

    return ExtractedAccessPlatform(
        alternativeTitle=alternative_title,
        contact=resolved_organigram,
        description=description,
        endpointType=endpoint_type,
        hadPrimarySource=get_extracted_primary_source_id_by_name("seq-repo"),
        identifierInPrimarySource=identifier_in_primary_source,
        landingPage=landing_page,
        technicalAccessibility=technical_accessibility,
        title=title,
        unitInCharge=resolved_organigram,
    )


def get_resolved_project_coordinator_units(
    project_coordinators: list[str],
) -> list[MergedOrganizationalUnitIdentifier]:
    """Get ldap resolved ids of project coordinators units.

    Args:
        project_coordinators: Seq Repo raw project coordinator names

    Returns:
        Resolved ids project coordinator units
    """
    department_number_by_project_coordinators = {
        pc: person.memberOf
        for pc in project_coordinators
        if (person := get_ldap_extracted_person_by_query(sam_account_name=pc))
    }
    return [
        department
        for pc in project_coordinators
        if pc in department_number_by_project_coordinators
        for department in department_number_by_project_coordinators[pc]
    ]

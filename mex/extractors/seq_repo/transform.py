from collections import defaultdict
from typing import TYPE_CHECKING

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.seq_repo.model import SeqRepoSource


def transform_seq_repo_activities_to_extracted_activities(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_extracted_persons_by_name: dict[str, ExtractedPerson],
) -> list[ExtractedActivity]:
    """Transform seq-repo activities to list of unique ExtractedActivity.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        seq_repo_activity: Seq Repo activity mapping models with default values
        seq_repo_extracted_persons_by_name: Seq Repo sources resolved project
                                            coordinators ldap query results

    Returns:
        list of unique ExtractedActivity
    """
    theme = seq_repo_activity.theme[0].mappingRules[0].setValues
    unique_activities = []

    for source in seq_repo_sources:
        project_coordinators_ids, responsible_units = (
            get_resolved_project_coordinators_and_units(
                source.project_coordinators,
                seq_repo_extracted_persons_by_name,
            )
        )

        if not responsible_units or not project_coordinators_ids:
            continue

        extracted_activity = ExtractedActivity(
            contact=project_coordinators_ids,
            hadPrimarySource=get_extracted_primary_source_id_by_name("seq-repo"),
            identifierInPrimarySource=source.project_id,
            involvedPerson=project_coordinators_ids,
            responsibleUnit=responsible_units,
            theme=theme,
            title=source.project_name,
        )

        if extracted_activity not in unique_activities:
            unique_activities.append(extracted_activity)

    return unique_activities


def transform_seq_repo_resource_to_extracted_resource(  # noqa: C901, PLR0913
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_activities: dict[str, ExtractedActivity],
    mex_access_platform: ExtractedAccessPlatform,
    seq_repo_resource: ResourceMapping,
    seq_repo_extracted_persons_by_name: dict[str, ExtractedPerson],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Transform seq-repo resources to ExtractedResource.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        seq_repo_activities: Seq Repo extracted activity for default values from mapping
        mex_access_platform: Extracted access platform
        seq_repo_resource: Seq Repo resource mapping model with default values
        seq_repo_extracted_persons_by_name: Seq Repo Sources resolved project
                                            coordinators as Extracted Persons
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
    sequence_dates_by_identifier_in_primary_source: dict[str, list[str]] = defaultdict(
        list
    )
    for source in seq_repo_sources:
        if source.sequencing_date:
            sequence_dates_by_identifier_in_primary_source[
                source.igs_id or source.lims_sample_id
            ].append(source.sequencing_date)
    seen: set[str] = set()
    for source in seq_repo_sources:
        identifier_in_primary_source = source.igs_id or source.lims_sample_id
        if identifier_in_primary_source in seen:
            continue
        seen.add(identifier_in_primary_source)
        sequencing_dates = sequence_dates_by_identifier_in_primary_source[
            identifier_in_primary_source
        ]
        modified = None
        start = None
        if sequencing_dates:
            modified = max(sequencing_dates)
            start = min(sequencing_dates)

        activity = seq_repo_activities.get(source.project_id)

        project_coordinators_ids, units_in_charge = (
            get_resolved_project_coordinators_and_units(
                source.project_coordinators,
                seq_repo_extracted_persons_by_name,
            )
        )

        if not units_in_charge or not project_coordinators_ids:
            continue
        contributing_unit = get_unit_merged_id_by_synonym(source.customer_org_unit_id)
        keyword = list(shared_keyword)
        if source.species:
            keyword.append(Text(value=source.species))
        if source.pathogen_code:
            keyword.append(Text(value=source.pathogen_code.removesuffix("P")))
        if source.sequencing_platform:
            keyword.append(Text(value=source.sequencing_platform))
        quality_information = [
            Text(value=f"Basepairs: {source.basepair_count}", language="en"),
            Text(value=f"Reads: {source.reads_count}", language="en"),
        ]
        if source.system_feedback:
            quality_information.append(
                Text(value=source.system_feedback, language="en")
            )
        title = f"LIMS Sample ID {source.lims_sample_id} ({source.species})"
        extracted_resource = ExtractedResource(
            accessPlatform=mex_access_platform.stableTargetId,
            accessRestriction=access_restriction,
            accrualPeriodicity=accrual_periodicity,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=project_coordinators_ids,
            contributingUnit=contributing_unit,
            description=description,
            hadPrimarySource=get_extracted_primary_source_id_by_name("seq-repo"),
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=keyword,
            modified=modified,
            publisher=extracted_organization_rki.stableTargetId,
            qualityInformation=quality_information,
            resourceCreationMethod=resource_creation_method,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource_type_specific,
            rights=rights,
            start=start,
            stateOfDataProcessing=state_of_data_processing,
            theme=theme,
            title=title,
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


def get_resolved_project_coordinators_and_units(
    project_coordinators: list[str],
    seq_repo_extracted_persons_by_name: dict[str, ExtractedPerson],
) -> tuple[list[MergedPersonIdentifier], list[MergedOrganizationalUnitIdentifier]]:
    """Get ldap resolved ids of project coordinators and units.

    Args:
        project_coordinators: Seq Repo raw project coordinator names
        seq_repo_extracted_persons_by_name: Seq Repo Sources resolved project
                                            coordinators as Extracted Persons

    Returns:
        Resolved ids project coordinator and units
    """
    project_coordinators_ids: set[MergedPersonIdentifier] = set()
    units_in_charge: set[MergedOrganizationalUnitIdentifier] = set()
    for pc in project_coordinators:
        person = seq_repo_extracted_persons_by_name.get(pc)
        if not person:
            continue
        project_coordinators_ids.add(person.stableTargetId)
        if unit := person.memberOf:
            units_in_charge.update(unit)

    return sorted(project_coordinators_ids), sorted(units_in_charge)

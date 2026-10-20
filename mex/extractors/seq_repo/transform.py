from collections import defaultdict
from functools import lru_cache
from typing import TYPE_CHECKING, cast

from mex.common.exceptions import MExError
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
    ExtractedResourceSeries,
    ResourceMapping,
    ResourceSeriesMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
)
from mex.extractors.ldap.helpers import (
    get_ldap_extracted_person_by_query,
    get_ldap_merged_contact_id_by_mail,
)
from mex.extractors.logging import watch_progress
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import ExtractorsSettings

if TYPE_CHECKING:
    from mex.extractors.seq_repo.model import SeqRepoSource


def transform_seq_repo_resource_to_extracted_resource_series(
    resource_series_mapping: ResourceSeriesMapping,
    seq_repo_sources: list[SeqRepoSource],
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResourceSeries]:
    """Transform seq-repo resources to ExtractedResourceSeries.

    Args:
        resource_series_mapping: Seq Repo resource series mapping with default values
        seq_repo_sources: Seq Repo extracted sources
        extracted_access_platform: Extracted access platform
        extracted_organization_rki: wikidata extracted organization

    Returns:
        list of ExtractedResourceSeries
    """
    access_platform = extracted_access_platform.stableTargetId
    description_template = resource_series_mapping.description.get_static_values()
    had_primary_source = get_extracted_primary_source_id_by_name("seq-repo")
    base_keywords = resource_series_mapping.keyword.get_static_values()
    publisher = extracted_organization_rki.stableTargetId

    if not description_template:
        msg = "Description in Resource Series mapping must contain setValues."
        raise MExError(msg)

    if not base_keywords:
        msg = "Keyword in Resource Series mapping must contain setValues."
        raise MExError(msg)

    collected_project_ids: set[str] = set()
    collected_project_name_by_project_id: dict[str, set[str]] = defaultdict(set)
    collected_keywords_by_project_id: dict[str, set[str]] = defaultdict(set)
    collected_project_coordinators_by_project_id: dict[str, set[str]] = defaultdict(set)
    collected_sequencing_dates_by_project_id: dict[str, set[str]] = defaultdict(set)
    for source in seq_repo_sources:
        project_id = source.project_id

        collected_project_ids.add(project_id)
        collected_project_name_by_project_id[project_id].add(source.project_name)
        collected_keywords = (
            source.species,
            source.pathogen_code.removesuffix("P") if source.pathogen_code else None,
            source.sequencing_platform,
        )
        collected_keywords_by_project_id[project_id].update(
            keyword for keyword in collected_keywords if keyword is not None
        )
        collected_project_coordinators_by_project_id[project_id].update(
            source.project_coordinators
        )
        if source.sequencing_date is not None:
            collected_sequencing_dates_by_project_id[project_id].add(
                source.sequencing_date
            )

    extracted_resource_series = []
    for project_id in sorted(collected_project_ids):
        contacts, _, _ = (
            resolve_project_coordinators_to_contacts_and_contributors_and_units(
                sorted(collected_project_coordinators_by_project_id[project_id])
            )
        )
        filtered_project_name_list = [
            name
            for name in sorted(collected_project_name_by_project_id[project_id])
            if name != "Other"
        ]
        project_name_text = f"'{filtered_project_name_list[0]}'"
        if additional_project_names := filtered_project_name_list[1:]:
            optional_project_names = ", ".join(
                f"'{name}'" for name in additional_project_names
            )
            project_name_text += f" (additional {optional_project_names})"
        description = [
            Text(
                value=template.value.replace("[project-name]", f"{project_name_text}"),
                language=template.language,
            )
            for template in description_template
        ]
        keyword = base_keywords + [
            Text(value=item)
            for item in sorted(collected_keywords_by_project_id[project_id])
        ]

        extracted_resource_series.append(
            ExtractedResourceSeries(
                accessPlatform=[access_platform],
                contact=contacts,
                description=description,
                end=max(collected_sequencing_dates_by_project_id[project_id]),
                hadPrimarySource=had_primary_source,
                identifierInPrimarySource=project_id,
                keyword=keyword,
                start=min(collected_sequencing_dates_by_project_id[project_id]),
                publisher=[publisher],
                title=sorted(collected_project_name_by_project_id[project_id]),
            )
        )
    return extracted_resource_series


def transform_seq_repo_resource_to_extracted_resource(
    seq_repo_sources: list[SeqRepoSource],
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_resource_series: list[ExtractedResourceSeries],
    resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Transform seq-repo resources to ExtractedResource.

    Args:
        seq_repo_sources: Seq Repo extracted sources
        extracted_access_platform: Extracted access platform
        extracted_resource_series: list of ExtractedResourceSeries
        resource_mapping: Seq Repo resource mapping model with default values
        extracted_organization_rki: wikidata extracted organization

    Returns:
        list of ExtractedResource
    """
    # Resource values from mapping
    access_restriction = resource_mapping.accessRestriction.get_static_values()
    accrual_periodicity = resource_mapping.accrualPeriodicity.get_static_values()
    anonymization_pseudonymization = (
        resource_mapping.anonymizationPseudonymization.get_static_values()
    )
    description = resource_mapping.description.get_static_values()
    health_category = resource_mapping.healthCategory.get_static_values()
    resource_creation_method = (
        resource_mapping.resourceCreationMethod.get_static_values()
    )
    resource_type_general = resource_mapping.resourceTypeGeneral.get_static_values()
    resource_type_specific = resource_mapping.resourceTypeSpecific.get_static_values()
    rights = resource_mapping.rights.get_static_values()
    state_of_data_processing = (
        resource_mapping.stateOfDataProcessing.get_static_values()
    )
    theme = resource_mapping.theme.get_static_values()
    shared_keyword = resource_mapping.keyword.get_static_values() or []

    extracted_resources = []
    sequence_dates_by_identifier_in_primary_source: dict[str, list[str]] = defaultdict(
        list
    )

    resource_series_merged_id_by_project_id = {
        rs.identifierInPrimarySource: rs.stableTargetId
        for rs in extracted_resource_series
    }

    for source in seq_repo_sources:
        if source.sequencing_date:
            sequence_dates_by_identifier_in_primary_source[
                source.igs_id or source.lims_sample_id
            ].append(source.sequencing_date)
    seen: set[str] = set()
    for source in watch_progress(
        seq_repo_sources, "transform_seq_repo_resource_to_extracted_resource"
    ):
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

        contacts, contributors, units = (
            resolve_project_coordinators_to_contacts_and_contributors_and_units(
                source.project_coordinators
            )
        )
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
            accessPlatform=extracted_access_platform.stableTargetId,
            accessRestriction=access_restriction,
            accrualPeriodicity=accrual_periodicity,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contacts,
            contributingUnit=contributing_unit,
            contributor=contributors,
            description=description,
            hadPrimarySource=get_extracted_primary_source_id_by_name("seq-repo"),
            healthCategory=health_category,
            identifierInPrimarySource=identifier_in_primary_source,
            keyword=keyword,
            inSeries=resource_series_merged_id_by_project_id[source.project_id],
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
            unitInCharge=units,
        )
        extracted_resources.append(extracted_resource)

    return extracted_resources


def transform_seq_repo_access_platform_to_extracted_access_platform(
    access_platform_mapping: AccessPlatformMapping,
) -> ExtractedAccessPlatform:
    """Transform seq-repo access platform to ExtractedAccessPlatform.

    Args:
        access_platform_mapping: Seq Repo access platform mapping model

    Returns:
        ExtractedAccessPlatform
    """
    alternative_title = access_platform_mapping.alternativeTitle.get_static_values()

    description = access_platform_mapping.description.get_static_values()
    endpoint_type = access_platform_mapping.endpointType.get_static_values()
    identifier_in_primary_source = (
        access_platform_mapping.identifierInPrimarySource.get_static_values()
    )
    landing_page = access_platform_mapping.landingPage.get_static_values()

    technical_accessibility = (
        access_platform_mapping.technicalAccessibility.get_static_values()
    )
    title = access_platform_mapping.title.get_static_values()

    contacts = access_platform_mapping.contact.get_mappable_values()

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


@lru_cache(maxsize=1024)
def extract_person_item_or_contact_point_id_by_name(
    project_coordinator: str,
) -> MergedContactPointIdentifier | ExtractedPerson | None:
    """Extract Persons by their query string for source project coordinators.

    Args:
        project_coordinator: string of project coordinator

    Returns:
        extracted person, contact id or None
    """
    if person := get_ldap_extracted_person_by_query(
        sam_account_name=project_coordinator
    ):
        return person
    return get_ldap_merged_contact_id_by_mail(mail=f"{project_coordinator}@rki.de")


def resolve_project_coordinators_to_contacts_and_contributors_and_units(
    project_coordinators: list[str],
) -> tuple[
    list[MergedContactPointIdentifier | MergedOrganizationalUnitIdentifier],
    list[MergedPersonIdentifier] | None,
    list[MergedOrganizationalUnitIdentifier],
]:
    """Resolve project coordinators to entries for contact, contributors and units.

    Use defined Fallback unit if no project coordinator could be resolved.

    Args:
        project_coordinators: Seq Repo raw project coordinator names

    Returns:
        Merged ids of contact points or persons or units
    """
    settings = ExtractorsSettings.get()
    contact_ids: set[
        MergedContactPointIdentifier | MergedOrganizationalUnitIdentifier
    ] = set()
    contributor_ids: set[MergedPersonIdentifier] = set()
    unit_ids: set[MergedOrganizationalUnitIdentifier] = set()
    for pc in project_coordinators:
        person_item_or_contact_id = extract_person_item_or_contact_point_id_by_name(pc)
        if not person_item_or_contact_id:
            continue
        if isinstance(person_item_or_contact_id, ExtractedPerson):
            contributor_ids.add(person_item_or_contact_id.stableTargetId)
            if unit := person_item_or_contact_id.memberOf:
                contact_ids.update(unit)
                unit_ids.update(unit)
        elif isinstance(person_item_or_contact_id, MergedContactPointIdentifier):
            contact_ids.add(person_item_or_contact_id)
        else:
            msg = f"Lookup returned unexpected entity type for entry {pc}."
            raise MExError(msg)

    if not unit_ids:
        unit_ids = cast(
            "set[MergedOrganizationalUnitIdentifier]",
            get_unit_merged_id_by_synonym(settings.seq_repo.fallback_unit),
        )
    if not contact_ids:
        contact_ids = cast(
            "set[MergedContactPointIdentifier | MergedOrganizationalUnitIdentifier]",
            get_unit_merged_id_by_synonym(settings.seq_repo.fallback_unit),
        )
    return sorted(contact_ids), sorted(contributor_ids), sorted(unit_ids)

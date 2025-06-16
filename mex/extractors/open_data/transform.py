import re

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import transform_ldap_person_to_mex_person
from mex.common.models import (
    ConsentMapping,
    DistributionMapping,
    ExtractedConsent,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import (
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    MIMEType,
)
from mex.extractors.open_data.extract import (
    extract_files_for_parent_resource,
    extract_oldest_record_version_creationdate,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def transform_open_data_person_affiliations_to_organisations(
    extracted_open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    extracted_primary_source_open_data: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Search wikidata or create own organisations, load to sink and create dictionary.

    Args:
        extracted_open_data_creators_contributors: list of creators and contributors
        extracted_primary_source_open_data: extracted Primary source for Open Data

    Returns:
        list of Extracted Organization Ids by affiliation name
    """
    unique_affiliations = {
        person.affiliation
        for person in extracted_open_data_creators_contributors
        if person.affiliation
    }
    affiliation_dict: dict[str, MergedOrganizationIdentifier] = {}
    for affiliation in unique_affiliations:
        if org_id := get_wikidata_extracted_organization_id_by_name(affiliation):
            affiliation_dict[affiliation] = org_id
        else:
            extracted_organization = ExtractedOrganization(
                officialName=affiliation,
                identifierInPrimarySource=affiliation,
                hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
            )
            load([extracted_organization])
            affiliation_dict[affiliation] = extracted_organization.stableTargetId
    return affiliation_dict


def transform_open_data_persons_not_in_ldap(
    person: OpenDataCreatorsOrContributors,
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_organization_rki: ExtractedOrganization,
    extracted_open_data_organizations: dict[str, MergedOrganizationIdentifier],
) -> ExtractedPerson:
    """Create ExtractedPerson for a person not matched with ldap.

    Args:
        person: list[OpenDataCreatorsOrContributors],
        extracted_primary_source_open_data: open data primary source,
        extracted_organization_rki: ExtractedOrganization of RKI,
        extracted_open_data_organizations: dictionary with ID by affiliation name

    Returns:
        ExtractedPerson
    """
    affiliation = (
        extracted_open_data_organizations[person.affiliation]
        if (
            person.affiliation
            and extracted_open_data_organizations[person.affiliation]
            != extracted_organization_rki.stableTargetId
        )
        else None
    )

    return ExtractedPerson(
        affiliation=affiliation,
        hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
        identifierInPrimarySource=person.name,
        fullName=person.name,
    )


def lookup_person_in_ldap_and_transfom(
    person: OpenDataCreatorsOrContributors,
    extracted_primary_source_ldap: ExtractedPrimarySource,
    units_by_identifier_in_primary_source: dict[str, ExtractedOrganizationalUnit],
) -> ExtractedPerson | None:
    """Lookup person in ldap. and transform to ExtractedPerson.

    Args:
        person: Open Data person (Creator Or Contributor),
        extracted_primary_source_ldap: primary Source for ldap
        units_by_identifier_in_primary_source: dict of primary sources by ID

    Returns:
        ExtractedPerson if matched or None if match fails
    """
    ldap = LDAPConnector.get()
    try:
        ldap_person = ldap.get_person(displayName=person.name)
        return transform_ldap_person_to_mex_person(
            ldap_person,
            extracted_primary_source_ldap,
            units_by_identifier_in_primary_source,
        )
    except MExError:
        return None


def transform_open_data_persons(  # noqa: PLR0913
    extracted_open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
    extracted_open_data_organizations: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedPerson]:
    """Lookup persons in ldap or create ExtractedPerson if match fails.

    Args:
        extracted_open_data_creators_contributors: list of Creators Or Contributors
        extracted_primary_source_ldap: Extracted Primary Sources for ldap
        extracted_primary_source_open_data: Extracted Primary Sources for open-data
        extracted_organizational_units: list of Extracted Organizational Units
        extracted_organization_rki: ExtractedOrganization of RKI,
        extracted_open_data_organizations: dictionary with ID by affiliation name

    Returns:
        list of Extracted Persons
    """
    units_by_identifier_in_primary_source = {
        unit.identifierInPrimarySource: unit for unit in extracted_organizational_units
    }

    extracted_persons: dict[MergedPersonIdentifier, ExtractedPerson] = {}

    for person in extracted_open_data_creators_contributors:
        extracted_person = lookup_person_in_ldap_and_transfom(
            person, extracted_primary_source_ldap, units_by_identifier_in_primary_source
        ) or transform_open_data_persons_not_in_ldap(
            person,
            extracted_primary_source_open_data,
            extracted_organization_rki,
            extracted_open_data_organizations,
        )

        if extracted_person.stableTargetId not in extracted_persons:
            extracted_persons[extracted_person.stableTargetId] = extracted_person

        if person.orcid:
            extracted_persons[extracted_person.stableTargetId].orcidId = [
                f"https://orcid.org/{person.orcid}"
            ]
    return list(extracted_persons.values())


def transform_persons_and_creation_date(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_open_data_persons: list[ExtractedPerson],
) -> dict[MergedPersonIdentifier, str]:
    """Extract file creation dates per Extracted Person from open_data resource.

    Args:
        open_data_resource_versions: Open Data resource versions
        extracted_open_data_persons: list of Extracted Persons

    Returns:
        dictionary of file Creation Date by stableTargetId of MergedPerson
    """
    dict_for_extractedconsent: dict[MergedPersonIdentifier, str] = {}
    extracted_person_ids_by_name = {
        extracted_person.fullName[0]: extracted_person.stableTargetId
        for extracted_person in extracted_open_data_persons
    }
    for resource in open_data_resource_versions:
        for person in resource.metadata.contributors + resource.metadata.creators:
            if person.name in extracted_person_ids_by_name:
                person_stable_target_id = extracted_person_ids_by_name[person.name]
                if (
                    resource.created
                    and (
                        person_stable_target_id in dict_for_extractedconsent
                        and dict_for_extractedconsent[person_stable_target_id]
                        > resource.created
                    )
                ) or (
                    resource.created
                    and (person_stable_target_id not in dict_for_extractedconsent)
                ):  # if saved creation date is newer or entry doesn't exist yet, update
                    dict_for_extractedconsent[person_stable_target_id] = (
                        resource.created
                    )
    return dict_for_extractedconsent


def transform_open_data_distributions(
    open_data_parent_resources: list[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    distribution_mapping: DistributionMapping,
) -> list[ExtractedDistribution]:
    """Transform open data resource versions to extracted distributions.

    Args:
        open_data_parent_resources: list of open data parent resources
        extracted_primary_source_open_data: Extracted platform for open data
        distribution_mapping: resource mapping model with default values

    Returns:
        List of ExtractedDistribution instances
    """
    extracted_distributions = []
    access_restriction = (
        distribution_mapping.accessRestriction[0].mappingRules[0].setValues
    )
    had_primary_source = extracted_primary_source_open_data.stableTargetId
    for resource in open_data_parent_resources:
        access_url = Link(url=f"https://doi.org/{resource.conceptdoi}")
        ccby_license = (
            distribution_mapping.license[0].mappingRules[0].setValues
            if distribution_mapping.license[0].mappingRules[0].forValues
            and str(resource.metadata.license.id)
            in distribution_mapping.license[0].mappingRules[0].forValues
            else None
        )
        for file in extract_files_for_parent_resource(resource.id):
            download_url = Link(url=file.links.self)
            identifier_primary_source = file.file_id
            issued = file.created
            media_type = MIMEType.find(str(file.mimetype))
            modified = file.updated
            title = file.key
            extracted_distributions.append(
                ExtractedDistribution(
                    accessRestriction=access_restriction,
                    accessURL=access_url,
                    downloadURL=download_url,
                    hadPrimarySource=had_primary_source,
                    identifierInPrimarySource=identifier_primary_source,
                    issued=issued,
                    license=ccby_license,
                    mediaType=media_type,
                    modified=modified,
                    title=title,
                )
            )
    return extracted_distributions


def transform_open_data_person_to_mex_consent(
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_persons_and_creation_date: dict[MergedPersonIdentifier, str],
    consent_mapping: ConsentMapping,
) -> list[ExtractedConsent]:
    """Transform open data persons to extracted consent.

    Args:
        extracted_primary_source_open_data: Extracted platform for open data
        extracted_open_data_persons: list of ExtractedPerson
        consent_mapping: resource mapping model with default values
        extracted_open_data_persons_and_creation_date: person id by file creation date

    Returns:
        List of ExtractedConsent instances
    """
    has_consent_status = consent_mapping.hasConsentStatus[0].mappingRules[0].setValues
    has_consent_type = consent_mapping.hasConsentType[0].mappingRules[0].setValues

    return [
        ExtractedConsent(
            hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
            hasConsentStatus=has_consent_status,
            hasConsentType=has_consent_type,
            hasDataSubject=person.stableTargetId,
            identifierInPrimarySource=person.stableTargetId + "_consent",
            isIndicatedAtTime=extracted_open_data_persons_and_creation_date[
                person.stableTargetId
            ],
        )
        for person in extracted_open_data_persons
    ]


def transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    open_data_parent_resource: list[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_open_data_distribution: list[ExtractedDistribution],
    resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    open_data_contact_point: list[ExtractedContactPoint],
) -> list[ExtractedResource]:
    """Transform open_data parent resources to extracted resources.

    Args:
        open_data_parent_resource: open data parent resources
        extracted_primary_source_open_data: Extracted platform for open data
        extracted_open_data_persons: list of ExtractedPerson
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_open_data_distribution: list of Extracted open data Distributions
        resource_mapping: resource mapping model with default values
        extracted_organization_rki: ExtractedOrganization
        open_data_contact_point: list[ExtractedContactPoint]

    Returns:
        list of ExtractedResource instances
    """
    extracted_resource = []

    person_stable_target_id_by_name = {
        str(p.fullName[0]): MergedPersonIdentifier(p.stableTargetId)
        for p in extracted_open_data_persons
    }
    unit_stable_target_ids_by_person_name = {
        p.fullName[0]: p.memberOf for p in extracted_open_data_persons
    }
    access_restriction = resource_mapping.accessRestriction[0].mappingRules[0].setValues
    anonymization_pseudonymization = (
        resource_mapping.anonymizationPseudonymization[0].mappingRules[0].setValues
    )
    contact_open_data = [contact.stableTargetId for contact in open_data_contact_point]
    distribution_by_id = {
        distribution.identifierInPrimarySource: distribution.stableTargetId
        for distribution in extracted_open_data_distribution
    }
    has_personal_data = resource_mapping.hasPersonalData[0].mappingRules[0].setValues
    resource_type_general = (
        resource_mapping.resourceTypeGeneral[0].mappingRules[0].setValues
    )
    theme = resource_mapping.theme[0].mappingRules[0].setValues
    language_by_keyword = {
        rule.forValues[0]: rule.setValues[0]
        for rule in resource_mapping.language[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for resource in open_data_parent_resource:
        contributing_unit = list(
            {
                unit_id
                for person in resource.metadata.contributors
                + resource.metadata.creators
                if (
                    unit_list := unit_stable_target_ids_by_person_name.get(
                        str(person.name)
                    )
                )
                for unit_id in unit_list
            }
        )
        contributor = [
            c
            for person in resource.metadata.contributors
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
        created = extract_oldest_record_version_creationdate(resource.id)
        creator = [
            c
            for person in resource.metadata.creators
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
        contact = contact_open_data + creator
        # remove html tags(<p>,</p>,<br>,<em>...), '\n' but keep <a href> and </a>
        description = (
            re.sub(
                r"<(?!/?a(?:\s+href)?)[^>]+>|\n", "", str(resource.metadata.description)
            ).strip()
            if resource.metadata.description
            else None
        )
        distribution = [distribution_by_id[str(file.id)] for file in resource.files]
        documentation = [
            Link(url=related_identifiers.identifier)
            for related_identifiers in resource.metadata.related_identifiers
            if related_identifiers.relation == "isDocumentedBy"
        ]
        doi = f"https://doi.org/{resource.conceptdoi}" if resource.conceptdoi else None
        language = (
            language_by_keyword[resource.metadata.language]
            if resource.metadata.language
            else None
        )
        ccby_license = (
            resource_mapping.license[0].mappingRules[0].setValues
            if resource_mapping.license[0].mappingRules[0].forValues
            and str(resource.metadata.license.id)
            in resource_mapping.license[0].mappingRules[0].forValues
            else None
        )
        unit_in_charge = (
            unit_stable_target_ids_by_synonym.get(
                resource_mapping.unitInCharge[0].mappingRules[0].forValues[0]
            )
            if resource_mapping.unitInCharge[0].mappingRules[0].forValues
            else None
        )
        extracted_resource.append(
            ExtractedResource(
                accessRestriction=access_restriction,
                anonymizationPseudonymization=anonymization_pseudonymization,
                contact=contact,
                contributingUnit=contributing_unit,
                contributor=contributor,
                created=created,
                creator=creator,
                description=description,
                distribution=distribution,
                documentation=documentation,
                doi=doi,
                hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
                hasPersonalData=has_personal_data,
                identifierInPrimarySource=str(resource.conceptrecid),
                keyword=resource.metadata.keywords,
                language=language,
                license=ccby_license,
                modified=resource.modified,
                publisher=extracted_organization_rki.stableTargetId,
                resourceTypeGeneral=resource_type_general,
                theme=theme,
                title=resource.title,
                unitInCharge=unit_in_charge,
            )
        )
    return extracted_resource

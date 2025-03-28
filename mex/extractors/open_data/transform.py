import re

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_person_to_mex_person,
)
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
    MexPersonAndCreationDate,
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
        orcidId=person.orcid,
    )


def lookup_person_in_ldap_and_transfom(
    person: OpenDataCreatorsOrContributors,
    extracted_primary_source_ldap: ExtractedPrimarySource,
    units_by_identifier_in_primary_source: dict[str, ExtractedOrganizationalUnit],
) -> ExtractedPerson | None:
    """Lookup person in ldap.

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
    except MExError:
        try:
            ldap_person = ldap.get_person(mail=(person.name.split(", ")[0] + "*"))
            # names are stored without Umlaut in Zenodo, therefore if the lookup
            # fails, try the email, as that also has no Umlauts. But one can't
            # use only this attempt because there are several similar last names
        except MExError:
            ldap_person = None

    if ldap_person:
        mex_person = transform_ldap_person_to_mex_person(
            ldap_person,
            extracted_primary_source_ldap,
            units_by_identifier_in_primary_source,
        )
        mex_person.orcidId = [person.orcid] if person.orcid else []
        return mex_person
    return None


def get_mex_person(  # noqa: PLR0913
    person: OpenDataCreatorsOrContributors,
    extracted_primary_source_ldap: ExtractedPrimarySource,
    units_by_identifier_in_primary_source: dict[str, ExtractedOrganizationalUnit],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_organization_rki: ExtractedOrganization,
    extracted_open_data_organizations: dict[str, MergedOrganizationIdentifier],
) -> ExtractedPerson:
    """Lookup person in ldap or create ExtractedPerson if match fails.

    Args:
        person: list[OpenDataCreatorsOrContributors],
        extracted_primary_source_ldap: primary Source for ldap
        units_by_identifier_in_primary_source: dict of primary sources by ID
        extracted_primary_source_open_data: open data primary source,
        extracted_organization_rki: ExtractedOrganization of RKI,
        extracted_open_data_organizations: dictionary with ID by affiliation name

    Returns:
        ExtractedPerson
    """
    mex_person = lookup_person_in_ldap_and_transfom(
        person, extracted_primary_source_ldap, units_by_identifier_in_primary_source
    )

    if not mex_person:
        mex_person = transform_open_data_persons_not_in_ldap(
            person,
            extracted_primary_source_open_data,
            extracted_organization_rki,
            extracted_open_data_organizations,
        )
    return mex_person


def transform_open_data_persons(  # noqa: PLR0913
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_open_data_organizations: dict[str, MergedOrganizationIdentifier],
    extracted_organization_rki: ExtractedOrganization,
) -> dict[MergedPersonIdentifier, MexPersonAndCreationDate]:
    """Extract persons and file creation dates from open_data resource.

    Extract the persons and their respective first creation date of all their files on
    open data. Lookup the Persons on LDAP and transform them to ExtractedPersons and
    return them and their first file creation date in a dictionary by name.

    Args:
        open_data_resource_versions: Open Data resource versions
        extracted_open_data_creators_contributors: unique persons from parent resources
        extracted_primary_source_ldap: ExtractedPrimarySource for ldap
        extracted_primary_source_open_data: ExtractedPrimarySource for open data
        extracted_organizational_units: list[ExtractedOrganizationalUnit]
        extracted_open_data_organizations: dictionary with ID by affiliation name
        extracted_organization_rki: ExtractedOrganization of RKI,

    Returns:
        dictionary of MexPersonAndCreationDate by stableTargetId of MergedPerson
    """
    dict_for_extractedconsent: dict[str, MexPersonAndCreationDate] = {}
    units_by_identifier_in_primary_source = {
        unit.identifierInPrimarySource: unit for unit in extracted_organizational_units
    }
    relevant_persons_by_name = {
        person.name: person for person in extracted_open_data_creators_contributors
    }
    for resource in open_data_resource_versions:
        for person in resource.metadata.contributors + resource.metadata.creators:
            if person.name in relevant_persons_by_name:
                if resource.created and (
                    person.name in dict_for_extractedconsent
                    and dict_for_extractedconsent[person.name].created
                    > resource.created
                ):
                    dict_for_extractedconsent[person.name].created = resource.created
                    continue
                person.orcid = (
                    f"https://orcid.org/{person.orcid}" if person.orcid else None
                )
                mex_person = get_mex_person(
                    person,
                    extracted_primary_source_ldap,
                    units_by_identifier_in_primary_source,
                    extracted_primary_source_open_data,
                    extracted_organization_rki,
                    extracted_open_data_organizations,
                )
                dict_for_extractedconsent[person.name] = MexPersonAndCreationDate(
                    mex_person=mex_person,
                    created=resource.created,
                )
    # now, instead of name, use stabletargetId of Person as key
    return {
        dict_for_extractedconsent[key].mex_person.stableTargetId: value
        for key, value in dict_for_extractedconsent.items()
    }


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
    extracted_open_data_persons_and_creation_date: dict[
        MergedPersonIdentifier, MexPersonAndCreationDate
    ],
    consent_mapping: ConsentMapping,
) -> list[ExtractedConsent]:
    """Transform open data persons to extracted consent.

    Args:
        extracted_primary_source_open_data: Extracted platform for open data
        extracted_open_data_persons: list of ExtractedPerson
        consent_mapping: resource mapping model with default values
        extracted_open_data_persons_and_creation_date: mex persons & file creation date

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
            ].created,
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

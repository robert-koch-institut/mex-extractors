import re
from collections.abc import Generator, Iterable

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_person_to_mex_person,
)
from mex.common.logging import watch
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
    Identifier,
    MergedOrganizationalUnitIdentifier,
    MIMEType,
)
from mex.extractors.open_data.extract import (
    extract_files_for_resource_version,
    extract_oldest_record_version_creationdate,
)
from mex.extractors.open_data.models.source import (
    MexPersonAndCreationDate,
    OpenDataParentResource,
    OpenDataResourceVersion,
)


def transform_open_data_persons(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, MexPersonAndCreationDate]:
    """Extract persons and file creation dates from open_data resource.

    Extract the persons and their respective first creation date of all their files on
    open data. Lookup the Persons on LDAP and transform them to ExtractedPersons and
    return them and their first file creation date in a dictionary by name.

    Args:
        open_data_resource_versions: Open Data resource versions
        extracted_primary_source_ldap: ExtractedPrimarySource for ldap
        extracted_organizational_units: list[ExtractedOrganizationalUnit]

    Returns:
        dictionary of MexPersonAndCreationDate by name
    """
    ldap = LDAPConnector.get()
    dict_for_extractedconsent: dict[str, MexPersonAndCreationDate] = {}
    units_by_identifier_in_primary_source = {
        unit.identifierInPrimarySource: unit for unit in extracted_organizational_units
    }
    for resource in open_data_resource_versions:
        for person in resource.metadata.creators + resource.metadata.contributors:
            if resource.created and (
                person.name in dict_for_extractedconsent
                and dict_for_extractedconsent[person.name].created > resource.created
            ):
                dict_for_extractedconsent[person.name].created = resource.created
                continue
            try:
                ldap_person = ldap.get_person(displayName=person.name)
            except MExError:
                try:
                    ldap_person = ldap.get_person(
                        mail=(person.name.split(", ")[0] + "*")
                    )
                    # names are stored without Umlaut in Zenodo, therefore if the lookup
                    # fails, try the email, as that also has no Umlauts. But one can't
                    # use only this attempt because there are several similar last names
                except MExError:
                    continue
            mex_person = transform_ldap_person_to_mex_person(
                ldap_person,
                extracted_primary_source_ldap,
                units_by_identifier_in_primary_source,
            )
            dict_for_extractedconsent[person.name] = MexPersonAndCreationDate(
                mex_person=mex_person,
                created=resource.created,
            )
    return dict_for_extractedconsent


@watch
def transform_open_data_distributions(
    open_data_resource_versions: list[OpenDataResourceVersion],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    distribution_mapping: DistributionMapping,
) -> Generator[ExtractedDistribution, None, None]:
    """Transform open data resource versions to extracted distributions.

    Args:
        open_data_resource_versions: open data resource versions
        extracted_primary_source_open_data: Extracted platform for open data
        distribution_mapping: resource mapping model with default values

    Returns:
        Generator for ExtractedDistribution instances
    """
    access_restriction = (
        distribution_mapping.accessRestriction[0].mappingRules[0].setValues
    )
    had_primary_source = extracted_primary_source_open_data.stableTargetId
    for resource in open_data_resource_versions:
        access_url = resource.doi_url
        if distribution_mapping.license[0].mappingRules[0].forValues and (
            str(resource.metadata.license.id)
            in distribution_mapping.license[0].mappingRules[0].forValues
        ):
            ccby_license = distribution_mapping.license[0].mappingRules[0].setValues
        for file in extract_files_for_resource_version(resource.id):
            download_url = file.links.self
            identifier_primary_source = file.file_id
            issued = file.created
            media_type = MIMEType.find(str(file.mimetype))
            modified = file.updated
            title = file.key
            yield ExtractedDistribution(
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


def transform_open_data_person_to_mex_consent(
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_persons_and_creation_date: dict[str, MexPersonAndCreationDate],
    consent_mapping: ConsentMapping,
) -> Generator[ExtractedConsent, None, None]:
    """Transform open data persons to extracted consent.

    Args:
        extracted_primary_source_open_data: Extracted platform for open data
        extracted_open_data_persons: list of ExtractedPerson
        consent_mapping: resource mapping model with default values
        extracted_open_data_persons_and_creation_date: mex persons & file creation date

    Returns:
        Generator for ExtractedConsent instances
    """
    has_consent_status = consent_mapping.hasConsentStatus[0].mappingRules[0].setValues
    has_consent_type = consent_mapping.hasConsentType[0].mappingRules[0].setValues
    person_list = list(extracted_open_data_persons_and_creation_date.values())
    person_filedate_by_person_stabletargetid = dict(
        zip(
            [person.mex_person.stableTargetId for person in person_list],
            [person.created for person in person_list],
            strict=False,
        )
    )
    for person in extracted_open_data_persons:
        yield ExtractedConsent(
            hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
            hasConsentStatus=has_consent_status,
            hasConsentType=has_consent_type,
            hasDataSubject=person.stableTargetId,
            identifierInPrimarySource=person.stableTargetId + "_consent",
            isIndicatedAtTime=person_filedate_by_person_stabletargetid[
                person.stableTargetId
            ],
        )


@watch
def transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    open_data_parent_resource: Iterable[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    open_data_contact_point: list[ExtractedContactPoint],
) -> Generator[ExtractedResource, None, None]:
    """Transform open_data parent resources to extracted resources.

    Args:
        open_data_parent_resource: open data parent resources
        extracted_primary_source_open_data: Extracted platform for open data
        extracted_open_data_persons: list of ExtractedPerson
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        resource_mapping: resource mapping model with default values
        extracted_organization_rki: ExtractedOrganization
        open_data_contact_point: list[ExtractedContactPoint]

    Returns:
        Generator for ExtractedResource instances
    """
    person_stable_target_id_by_name = {
        str(p.fullName[0]): Identifier(p.stableTargetId)
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
    has_personal_data = resource_mapping.hasPersonalData[0].mappingRules[0].setValues
    resource_type_general = (
        resource_mapping.resourceTypeGeneral[0].mappingRules[0].setValues
    )
    theme = resource_mapping.theme[0].mappingRules[0].setValues
    for resource in open_data_parent_resource:
        contact = contact_open_data + [
            c
            for person in resource.metadata.creators
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
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
        if resource.metadata.description:
            # remove html tags(<p>,</p>,<br>,<em>...), '\n' but keep <a href> and </a>
            description = re.sub(
                r"<(?!/?a(?:\s+href)?)[^>]+>|\n", "", str(resource.metadata.description)
            ).strip()
        else:
            description = None
        documentation = [
            related_identifiers.identifier
            for related_identifiers in resource.metadata.related_identifiers
            if related_identifiers.relation == "isDocumentedBy"
        ]
        doi = f"https://doi.org/{resource.conceptdoi}" if resource.conceptdoi else None
        language = None
        for mapping in resource_mapping.language[0].mappingRules:
            if mapping.forValues and resource.metadata.language == mapping.forValues[0]:
                language = mapping.setValues
        if resource_mapping.license[0].mappingRules[0].forValues and (
            resource.metadata.license.id
            in resource_mapping.license[0].mappingRules[0].forValues
        ):
            ccby_license = resource_mapping.license[0].mappingRules[0].setValues
        if resource_mapping.unitInCharge[0].mappingRules[0].forValues:
            unit_in_charge = unit_stable_target_ids_by_synonym.get(
                resource_mapping.unitInCharge[0].mappingRules[0].forValues[0]
            )
        else:
            unit_in_charge = None
        yield ExtractedResource(
            accessRestriction=access_restriction,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            created=created,
            creator=creator,
            description=description,
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


@watch
def transform_open_data_resource_version_to_mex_resource(  # noqa: PLR0913
    open_data_resource_versions: Iterable[OpenDataResourceVersion],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_parent_resources: list[ExtractedResource],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_open_data_distribution: list[ExtractedDistribution],
    resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    open_data_contact_point: list[ExtractedContactPoint],
) -> Generator[ExtractedResource, None, None]:
    """Transform open_data resources to extracted resources.

    Args:
        open_data_resource_versions: open data resource versions
        extracted_primary_source_open_data: ExtractedPrimarySource for open data
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_open_data_persons: list of ExtractedPerson for open data
        extracted_open_data_parent_resources: list of ExtractedResources
        extracted_open_data_distribution: list of Extracted open data Distributions
        resource_mapping: resource mapping model with default values
        extracted_organization_rki: ExtractedOrganization
        open_data_contact_point: list[ExtractedContactPoint]

    Returns:
        Generator for ExtractedResource instances
    """
    person_stable_target_id_by_name = {
        str(p.fullName[0]): Identifier(p.stableTargetId)
        for p in extracted_open_data_persons
    }
    unit_stable_target_ids_by_person_name = {
        p.fullName[0]: p.memberOf for p in extracted_open_data_persons
    }
    parent_resource_stable_target_ids_by_conceptrecid = {
        r.identifierInPrimarySource: Identifier(r.stableTargetId)
        for r in extracted_open_data_parent_resources
    }
    access_restriction = resource_mapping.accessRestriction[0].mappingRules[0].setValues
    anonymization_pseudonymization = (
        resource_mapping.anonymizationPseudonymization[0].mappingRules[0].setValues
    )
    contact_open_data = [contact.stableTargetId for contact in open_data_contact_point]
    distribution_by_id = dict(
        zip(
            [
                distribution.identifierInPrimarySource
                for distribution in extracted_open_data_distribution
            ],
            [
                distribution.stableTargetId
                for distribution in extracted_open_data_distribution
            ],
            strict=False,
        )
    )
    has_personal_data = resource_mapping.hasPersonalData[0].mappingRules[0].setValues
    resource_type_general = (
        resource_mapping.resourceTypeGeneral[0].mappingRules[0].setValues
    )
    theme = resource_mapping.theme[0].mappingRules[0].setValues
    for resource in open_data_resource_versions:
        contact = contact_open_data + [
            c
            for person in resource.metadata.creators
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
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
        creator = [
            c
            for person in resource.metadata.creators
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
        if resource.metadata.description:
            # remove html tags(<p>,</p>,<br>,<em>...), '\n' but keep <a href> and </a>
            description = re.sub(
                r"<(?!/?a(?:\s+href)?)[^>]+>|\n", "", str(resource.metadata.description)
            ).strip()
        else:
            description = None
        distribution = [distribution_by_id[str(file.id)] for file in resource.files]
        documentation = [
            related_identifiers.identifier
            for related_identifiers in resource.metadata.related_identifiers
            if related_identifiers.relation == "isDocumentedBy"
        ]
        if resource_mapping.license[0].mappingRules[0].forValues and (
            resource.metadata.license.id
            in resource_mapping.license[0].mappingRules[0].forValues
        ):
            ccby_license = resource_mapping.license[0].mappingRules[0].setValues
        is_part_of = parent_resource_stable_target_ids_by_conceptrecid.get(
            resource.conceptrecid
        )
        language = None
        for mapping in resource_mapping.language[0].mappingRules:
            if mapping.forValues and resource.metadata.language == mapping.forValues[0]:
                language = mapping.setValues
        if resource_mapping.unitInCharge[0].mappingRules[0].forValues:
            unit_in_charge = unit_stable_target_ids_by_synonym.get(
                resource_mapping.unitInCharge[0].mappingRules[0].forValues[0]
            )
        else:
            unit_in_charge = None
        yield ExtractedResource(
            accessRestriction=access_restriction,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            created=resource.created,
            creator=creator,
            description=description,
            distribution=distribution,
            documentation=documentation,
            doi=resource.doi_url,
            hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
            hasPersonalData=has_personal_data,
            identifierInPrimarySource=str(resource.id),
            license=ccby_license,
            modified=resource.modified,
            isPartOf=is_part_of,
            keyword=resource.metadata.keywords,
            language=language,
            publisher=extracted_organization_rki.stableTargetId,
            resourceTypeGeneral=resource_type_general,
            theme=theme,
            title=resource.title,
            unitInCharge=unit_in_charge,
        )

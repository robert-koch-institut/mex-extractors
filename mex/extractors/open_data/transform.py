import re
from collections.abc import Generator, Iterable

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPerson
from mex.common.logging import watch
from mex.common.models import (
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
)
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
)
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.open_data.extract import extract_oldest_record_version_creationdate
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)


@watch
def transform_open_data_persons(
    open_data_resource_versions: Iterable[OpenDataResourceVersion],
) -> Generator[LDAPPerson, None, None]:
    """Extract LDAP persons from open_data resource.

    Args:
        open_data_resource_versions: Open Data resource versions

    Returns:
        Generator for LDAP persons
    """
    ldap = LDAPConnector.get()
    seen = set()
    for resource in open_data_resource_versions:
        for person in resource.metadata.creators + resource.metadata.contributors:
            if person in seen:
                continue
            try:
                yield ldap.get_person(displayName=str(person.name))
                seen.add(person)
            except MExError:
                continue


@watch
def transform_open_data_parent_resource_to_mex_resource(
    open_data_parent_resource: Iterable[OpenDataParentResource],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    resource_mapping: AnyMappingModel,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> Generator[ExtractedResource, None, None]:
    """Transform open_data parent resources to extracted resources.

    Args:
        open_data_parent_resource: open data parent versions
        extracted_primary_source_open_data: Extracted platform for open data
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_open_data_persons: list of ExtractedPerson
        resource_mapping: resource mapping model with default values

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
    contact_open_data = (  # ldap = LDAPConnector.get()
        "opendatarkidede"  # ldap.get_functional_account(mail="opendata@rki.de")
    )
    has_personal_data = resource_mapping.hasPersonalData[0].mappingRules[0].setValues
    resource_type_general = (
        resource_mapping.resourceTypeGeneral[0].mappingRules[0].setValues
    )
    theme = resource_mapping.theme[0].mappingRules[0].setValues
    for resource in open_data_parent_resource:
        contact = [contact_open_data] + [
            c
            for person in resource.metadata.creators
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
        contributing_unit = [
            unit_id
            for person in resource.metadata.contributors + resource.metadata.creators
            if (
                unit_list := unit_stable_target_ids_by_person_name.get(str(person.name))
            )
            for unit_id in unit_list
        ]
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
        description = (
            re.sub(
                r"<(?!/?a(?:\s+href)?)[^>]+>", "", str(resource.metadata.description)
            ).strip()
        )  # remove html tags(<p>,</p>,<br>,<em>...) and '/n' but keep <a href> and </a>
        documentation = [
            related_identifiers.identifier
            for related_identifiers in resource.metadata.related_identifiers
            if related_identifiers.relation == "isDocumentedBy"
        ]
        for mapping in resource_mapping.language[0].mappingRules:
            if resource.metadata.language == mapping.forValues[0]:
                language = mapping.setValues[0]
        if (
            resource.metadata.license.id
            in resource_mapping.license[0].mappingRules[0].forValues
        ):
            ccby_license = resource_mapping.license[0].mappingRules[0].setValues
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
            doi=f"https://doi.org/{resource.conceptdoi}",
            hadPrimarySource=extracted_primary_source_open_data.stableTargetId,
            hasPersonalData=has_personal_data,
            identifierInPrimarySource=str(resource.conceptrecid),
            keyword=resource.metadata.keywords,
            language=language,
            license=ccby_license,
            modified=resource.modified,
            publisher=unit_stable_target_ids_by_synonym.get("rki"),
            resourceTypeGeneral=resource_type_general,
            theme=theme,
            title=resource.title,
            unitInCharge=unit_stable_target_ids_by_synonym.get("mf4"),
        )


@watch
def transform_open_data_resource_version_to_mex_resource(  # noqa: PLR0913
    open_data_resource_versions: Iterable[OpenDataResourceVersion],
    extracted_primary_source_open_data: ExtractedPrimarySource,
    extracted_open_data_persons: list[ExtractedPerson],
    extracted_open_data_parent_resources: list[ExtractedResource],
    resource_mapping: AnyMappingModel,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> Generator[ExtractedResource, None, None]:
    """Transform open_data resources to extracted resources.

    Args:
        open_data_resource_versions: open data resource versions
        extracted_primary_source_open_data: Extracted platform for open data
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_open_data_persons: list of ExtractedPerson
        extracted_open_data_parent_resources: list of ExtractedResources
        resource_mapping: resource mapping model with default values

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
    contact_open_data = (  # ldap = LDAPConnector.get()
        "opendatarkidede"  # ldap.get_functional_account(mail="opendata@rki.de")
    )
    has_personal_data = resource_mapping.hasPersonalData[0].mappingRules[0].setValues
    resource_type_general = (
        resource_mapping.resourceTypeGeneral[0].mappingRules[0].setValues
    )
    theme = resource_mapping.theme[0].mappingRules[0].setValues
    for resource in open_data_resource_versions:
        contact = [contact_open_data] + [
            c
            for person in resource.metadata.creators
            if (c := person_stable_target_id_by_name.get(str(person.name)))
        ]
        contributing_unit = [
            unit_id
            for person in resource.metadata.contributors + resource.metadata.creators
            if (
                unit_list := unit_stable_target_ids_by_person_name.get(str(person.name))
            )
            for unit_id in unit_list
        ]
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
        description = (
            re.sub(
                r"<(?!/?a(?:\s+href)?)[^>]+>", "", str(resource.metadata.description)
            ).strip()
        )  # remove html tags(<p>,</p>,<br>,<em>...) and '/n' but keep <a href> and </a>
        documentation = [
            related_identifiers.identifier
            for related_identifiers in resource.metadata.related_identifiers
            if related_identifiers.relation == "isDocumentedBy"
        ]
        if (
            resource.metadata.license.id
            in resource_mapping.license[0].mappingRules[0].forValues
        ):
            ccby_license = resource_mapping.license[0].mappingRules[0].setValues
        is_part_of = parent_resource_stable_target_ids_by_conceptrecid.get(
            resource.conceptrecid
        )
        for mapping in resource_mapping.language[0].mappingRules:
            if resource.metadata.language == mapping.forValues[0]:
                language = mapping.setValues[0]
        yield ExtractedResource(
            accessRestriction=access_restriction,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            created=resource.created,
            creator=creator,
            description=description,  # distribution=distribution
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
            publisher=unit_stable_target_ids_by_synonym.get("rki"),
            resourceTypeGeneral=resource_type_general,
            theme=theme,
            title=resource.title,
            unitInCharge=unit_stable_target_ids_by_synonym.get("mf4"),
        )

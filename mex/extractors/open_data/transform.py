import re
from functools import lru_cache
from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.common.models import (
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
    MIMEType,
)
from mex.extractors.ldap.helpers import (
    get_ldap_extracted_person_by_query,
)
from mex.extractors.open_data.extract import (
    extract_files_for_parent_resource,
    extract_oldest_record_version_creationdate,
)
from mex.extractors.organigram.helpers import (
    get_extracted_organizational_units,
    get_unit_merged_id_by_synonym,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.open_data.models.source import (
        OpenDataCreatorsOrContributors,
        OpenDataParentResource,
        OpenDataTableSchema,
    )

# TODO @MX-2075: remove
FALLBACK_UNIT = "mf4"


def get_unit_ids_of_parent_units() -> set[MergedOrganizationalUnitIdentifier]:
    """Return set of all units that are parent units.

    Return:
        set of ids of parent units
    """
    extracted_organizational_units = get_extracted_organizational_units()
    return {
        unit.parentUnit for unit in extracted_organizational_units if unit.parentUnit
    }


@lru_cache(maxsize=1024)
def has_no_child_units(
    unit_id: MergedOrganizationalUnitIdentifier,
) -> bool:
    """Check whether unit has child units.

    Args:
        unit_id: unit_id_to_check

    Returns:
        True if unit has no child else false
    """
    return unit_id not in get_unit_ids_of_parent_units()


def transform_open_data_person_affiliations_to_organizations(
    open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search wikidata or create own organizations, load to sink and create dictionary.

    Args:
        open_data_creators_contributors: list of creators and contributors

    Returns:
        list of Extracted Organization Ids by affiliation name
    """
    unique_affiliations = {
        person.affiliation: None
        for person in open_data_creators_contributors
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
                hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
            )
            load([extracted_organization])
            affiliation_dict[affiliation] = extracted_organization.stableTargetId
    return affiliation_dict


lru_cache(maxsize=1024)


def transform_and_load_open_data_persons_not_in_ldap(
    person: OpenDataCreatorsOrContributors,
    open_data_organization_ids_by_str: dict[str, MergedOrganizationIdentifier],
) -> ExtractedPerson:
    """Create ExtractedPerson for a person not matched with ldap.

    Args:
        person: list[OpenDataCreatorsOrContributors],
        open_data_organization_ids_by_str: dictionary with ID by affiliation name

    Returns:
        ExtractedPerson
    """
    affiliation = (
        open_data_organization_ids_by_str[person.affiliation]
        if (
            person.affiliation
            and open_data_organization_ids_by_str[person.affiliation]
            != get_wikidata_extracted_organization_id_by_name("RKI")
        )
        else None
    )

    extracted_person = ExtractedPerson(
        affiliation=affiliation,
        hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
        identifierInPrimarySource=person.name,
        fullName=person.name,
        orcidId=f"https://orcid.org/{person.orcid}" or None,
    )
    load([extracted_person])
    return extracted_person


def get_or_transform_open_data_persons(
    open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    open_data_organization_ids_by_str: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedPerson]:
    """Lookup persons in ldap or create ExtractedPerson if match fails.

    Args:
        open_data_creators_contributors: list of Creators Or Contributors
        open_data_organization_ids_by_str: dictionary with ID by affiliation name

    Returns:
        list of Extracted Persons
    """
    extracted_persons: list[ExtractedPerson] = []

    for person in open_data_creators_contributors:
        if ldap_person_id := get_ldap_extracted_person_by_query(
            display_name=person.name
        ):
            extracted_person = ldap_person_id
        else:
            extracted_person = transform_and_load_open_data_persons_not_in_ldap(
                person,
                open_data_organization_ids_by_str,
            )

        if extracted_person not in extracted_persons:
            extracted_persons.append(extracted_person)

    return extracted_persons


def transform_open_data_distributions(
    open_data_parent_resources: list[OpenDataParentResource],
    distribution_mapping: DistributionMapping,
) -> list[ExtractedDistribution]:
    """Transform open data resource versions to extracted distributions.

    Args:
        open_data_parent_resources: list of open data parent resources
        distribution_mapping: resource mapping model with default values

    Returns:
        List of ExtractedDistribution instances
    """
    extracted_distributions = []
    access_restriction = (
        distribution_mapping.accessRestriction[0].mappingRules[0].setValues
    )
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
                    hadPrimarySource=get_extracted_primary_source_id_by_name(
                        "open-data"
                    ),
                    identifierInPrimarySource=identifier_primary_source,
                    issued=issued,
                    license=ccby_license,
                    mediaType=media_type,
                    modified=modified,
                    title=title,
                )
            )
    return extracted_distributions


def transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    open_data_parent_resource: list[OpenDataParentResource],
    open_data_extracted_persons: list[ExtractedPerson],
    open_data_distribution: list[ExtractedDistribution],
    resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    open_data_extracted_contact_points: list[ExtractedContactPoint],
) -> list[ExtractedResource]:
    """Transform open_data parent resources to extracted resources.

    Args:
        open_data_parent_resource: open data parent resources
        open_data_extracted_persons: list y name
        open_data_distribution: list of Extracted open data Distributions
        resource_mapping: resource mapping model with default values
        extracted_organization_rki: ExtractedOrganization
        open_data_extracted_contact_points: list of ExtractedContactPoints

    Returns:
        list of ExtractedResource instances
    """
    if not (fallback_unit_id := get_unit_merged_id_by_synonym(FALLBACK_UNIT)):
        msg = f"No ID found for {FALLBACK_UNIT}"
        raise MExError(msg)

    extracted_resource = []
    person_stable_target_id_by_name = {
        str(p.fullName[0]): MergedPersonIdentifier(p.stableTargetId)
        for p in open_data_extracted_persons
    }
    unit_stable_target_ids_by_person_name = {
        p.fullName[0]: [
            department for department in p.memberOf if has_no_child_units(department)
        ]
        for p in open_data_extracted_persons
    }
    access_restriction = resource_mapping.accessRestriction[0].mappingRules[0].setValues
    anonymization_pseudonymization = (
        resource_mapping.anonymizationPseudonymization[0].mappingRules[0].setValues
    )
    contact_open_data = [
        contact.stableTargetId for contact in open_data_extracted_contact_points
    ]
    distribution_by_id = {
        distribution.identifierInPrimarySource: distribution.stableTargetId
        for distribution in open_data_distribution
    }
    has_personal_data = resource_mapping.hasPersonalData[0].mappingRules[0].setValues
    resource_type_general_lookup = {
        rule.forValues[0]: rule.setValues
        for rule in resource_mapping.resourceTypeGeneral[0].mappingRules
        if rule.forValues
    }
    theme = resource_mapping.theme[0].mappingRules[0].setValues
    language_by_keyword = {
        rule.forValues[0]: rule.setValues[0]
        for rule in resource_mapping.language[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for resource in open_data_parent_resource:
        unit_in_charge = list(
            {
                unit_id: None
                for person in resource.metadata.contributors
                + resource.metadata.creators
                if (
                    unit_list := unit_stable_target_ids_by_person_name.get(
                        str(person.name)
                    )
                )
                for unit_id in unit_list
                if unit_id not in fallback_unit_id
            }
        )
        if not unit_in_charge:
            unit_in_charge = fallback_unit_id
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
        contributing_unit = (
            get_unit_merged_id_by_synonym(
                resource_mapping.contributingUnit[0].mappingRules[0].forValues[0]
            )
            if resource_mapping.contributingUnit[0].mappingRules[0].forValues
            else []
        )
        resource_type_general = resource_type_general_lookup.get(
            resource.metadata.resource_type.type, []
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
                hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
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


def transform_open_data_variable_groups(
    open_data_tableschemas_by_resource_id: dict[
        MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]
    ],
) -> list[ExtractedVariableGroup]:
    """Transform zip table schema names to variable groups.

    Args:
        open_data_tableschemas_by_resource_id: list of table schemas by name by resource

    Returns:
        extracted variable groups
    """
    return [
        ExtractedVariableGroup(
            hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
            identifierInPrimarySource=filename,
            containedBy=resource_id,
            label=filename.removesuffix(".json"),
        )
        for resource_id, schema_dict in open_data_tableschemas_by_resource_id.items()
        for filename in schema_dict
    ]


def transform_open_data_variables(
    open_data_tableschemas_by_resource_id: dict[
        MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]
    ],
    merged_variable_group_id_by_filename: dict[str, MergedVariableGroupIdentifier],
) -> list[ExtractedVariable]:
    """Transform table schema content to variables.

    Args:
        open_data_tableschemas_by_resource_id: list of table schemas by name by resource
        merged_variable_group_id_by_filename: variable group stableTargetId by filename

    Returns:
        extracted variables
    """
    extracted_variables: list[ExtractedVariable] = []
    for resource_id, schema_dict in open_data_tableschemas_by_resource_id.items():
        for filename in schema_dict:
            for schema in schema_dict[filename]:
                value_set = (
                    [f"{item.value}, {item.label}" for item in schema.categories]
                    if schema.categories
                    else (
                        [str(item) for item in schema.constraints.enum]
                        if schema.constraints and schema.constraints.enum
                        else None
                    )
                )
                extracted_variables.append(
                    ExtractedVariable(
                        hadPrimarySource=get_extracted_primary_source_id_by_name(
                            "open-data"
                        ),
                        identifierInPrimarySource=f"{schema.name}_{filename}",
                        dataType=schema.type,
                        label=[schema.name],
                        usedIn=[resource_id],
                        belongsTo=[merged_variable_group_id_by_filename[filename]],
                        description=[schema.description],
                        valueSet=value_set,
                    )
                )
    return extracted_variables

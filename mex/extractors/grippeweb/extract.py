from typing import TYPE_CHECKING, Any

from mex.extractors.grippeweb.connector import QUERY_BY_TABLE_NAME, GrippewebConnector
from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import AccessPlatformMapping, ResourceMapping
    from mex.common.types import (
        MergedContactPointIdentifier,
        MergedOrganizationIdentifier,
        MergedPersonIdentifier,
    )


def extract_columns_by_table_and_column_name() -> dict[str, dict[str, list[Any]]]:
    """Extract sql tables and parse them into column lists by table and column name.

    Returns:
        list of columns by column names and table names
    """
    connection = GrippewebConnector.get()
    return {
        table_name: connection.parse_columns_by_column_name(table_name)
        for table_name in QUERY_BY_TABLE_NAME
    }


def extract_ldap_actors_for_functional_accounts(
    grippeweb_resource_mappings: list[ResourceMapping],
) -> dict[str, MergedContactPointIdentifier]:
    """Extract LDAP actors functional accounts from grippeweb resource mapping contacts.

    Args:
        grippeweb_resource_mappings: list of resources default value mapping models

    Returns:
       merged contact identifier by mail
    """
    return {
        mail: contact_id
        for mapping in grippeweb_resource_mappings
        for mail in (mapping.contact[0].mappingRules[0].forValues or [])
        if (contact_id := get_ldap_merged_contact_id_by_mail(mail=mail))
    }


def extract_grippeweb_ldap_person_ids_by_query(
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_access_platform: AccessPlatformMapping,
) -> dict[str, MergedPersonIdentifier]:
    """Extract LDAP persons for grippeweb.

    Args:
        grippeweb_resource_mappings: list of resources default value mapping models
        grippeweb_access_platform: grippeweb access platform mapping model

    Returns:
        LDAP persons by name or mail
    """
    return {
        name: person_id
        for mapping in grippeweb_resource_mappings
        for name in (mapping.contributor[0].mappingRules[0].forValues or [])
        if (
            person_id := get_ldap_merged_person_id_by_query(
                given_name=name.split(" ")[0], surname=name.split(" ")[1]
            )
        )
    } | {
        mail: person_id
        for mail in (
            grippeweb_access_platform.contact[0].mappingRules[0].forValues or []
        )
        if (person_id := get_ldap_merged_person_id_by_query(mail=mail))
    }


def extract_grippeweb_organizations(
    grippeweb_resource_mappings: list[ResourceMapping],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract grippeweb organization from wikidata.

    Args:
        grippeweb_resource_mappings: grippeweb resource mapping models

    Returns:
        Dict with keys: mapping default values
            and values: MergedOrganizationIdentifier
    """
    organization_by_name = {}
    for resource in grippeweb_resource_mappings:
        if external_partner_dict := resource.externalPartner:
            external_partner = external_partner_dict[0].mappingRules[0].forValues[0]  # type: ignore[index]
            if org := get_wikidata_extracted_organization_id_by_name(external_partner):
                organization_by_name[external_partner] = org
        publisher_name = resource.publisher[0].mappingRules[0].forValues[0]  # type: ignore[index]
        if publisher := get_wikidata_extracted_organization_id_by_name(publisher_name):
            organization_by_name[publisher_name] = publisher
    return organization_by_name

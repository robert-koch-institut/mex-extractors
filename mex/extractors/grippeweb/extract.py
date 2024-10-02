from typing import Any

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.actor import LDAPActor
from mex.common.ldap.models.person import LDAPPerson
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.extractors.grippeweb.connector import QUERY_BY_TABLE_NAME, GrippewebConnector


def extract_columns_by_table_and_column_name() -> dict[str, dict[str, list[Any]]]:
    """Extract sql tables and parse them into column lists by table and column name.

    Returns:
        list of columns by column names and table names
    """
    connection = GrippewebConnector.get()
    return {
        table_name: connection.parse_columns_by_column_name(table_name)
        for table_name in QUERY_BY_TABLE_NAME.keys()
    }


def extract_ldap_actors_for_functional_accounts(
    grippeweb_resource_mappings: list[dict[str, Any]],
) -> list[LDAPActor]:
    """Extract LDAP actors functional accounts from grippeweb resource mapping contacts.

    Args:
        grippeweb_resource_mappings: list of resources default value dicts

    Returns:
        list of LDAP actors
    """
    ldap = LDAPConnector.get()

    return [
        next(ldap.get_functional_accounts(mail))
        for mapping in grippeweb_resource_mappings
        for mail in mapping["contact"][0]["mappingRules"][0]["forValues"]
    ]


def extract_ldap_persons(
    grippeweb_resource_mappings: list[dict[str, Any]],
    grippeweb_access_platform: dict[str, Any],
) -> list[LDAPPerson]:
    """Extract LDAP persons for grippeweb.

    Args:
        grippeweb_resource_mappings: list of resources default value dicts
        grippeweb_access_platform: grippeweb access platform

    Returns:
        list of LDAP persons
    """
    ldap = LDAPConnector.get()
    return [
        *[
            ldap.get_person(given_name=name.split(" ")[0], surname=name.split(" ")[1])
            for mapping in grippeweb_resource_mappings
            for name in mapping["contributor"][0]["mappingRules"][0]["forValues"]
        ],
        *[
            ldap.get_person(mail=mail)
            for mail in grippeweb_access_platform["contact"][0]["mappingRules"][0][
                "forValues"
            ]
        ],
    ]


def extract_grippeweb_organizations(
    grippeweb_resource_mappings: list[dict[str, Any]],
) -> dict[str, WikidataOrganization]:
    """Search and extract grippeweb organization from wikidata.

    Args:
        grippeweb_resource_mappings: grippeweb resource mappings

    Returns:
        Dict with keys: mapping default values
            and values: WikidataOrganization
    """
    publisher_by_name = {}
    for resource in grippeweb_resource_mappings:
        publisher_name = str(
            resource["publisher"][0]["mappingRules"][0]["forValues"][0]
        )
        if publisher := search_organization_by_label(publisher_name):
            publisher_by_name[publisher_name] = publisher
    return publisher_by_name

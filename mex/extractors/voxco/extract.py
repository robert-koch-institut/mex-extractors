from typing import TYPE_CHECKING

from mex.common.ldap.connector import LDAPConnector
from mex.extractors.drop import DropApiConnector
from mex.extractors.voxco.model import VoxcoVariable
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.ldap.models import LDAPPerson
    from mex.common.models import ResourceMapping
    from mex.common.types import MergedOrganizationIdentifier


def extract_voxco_variables() -> dict[str, list[VoxcoVariable]]:
    """Extract voxco variables by loading data from mex-drop source json file.

    Returns:
        lists of voxco variables by json file name
    """
    connector = DropApiConnector.get()
    files = connector.list_files("voxco")
    data = {
        file_name: connector.get_file("voxco", file_name)
        for file_name in files
        if "test_" not in file_name
    }
    return {
        file_name.removesuffix(".json"): [
            VoxcoVariable.model_validate(item) for item in file_rows["value"]
        ]
        for file_name, file_rows in data.items()
    }


def extract_voxco_organizations(
    voxco_resource_mappings: list[ResourceMapping],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract voxco organization from wikidata.

    Args:
        voxco_resource_mappings: voxco resource mapping models

    Returns:
        Dict with organization label and MergedOrganizationIdentifier
    """
    voxco_resource_organizations = {}
    external_partners = [
        mapping.externalPartner[0].mappingRules[0].forValues[0]  # type: ignore[index]
        for mapping in voxco_resource_mappings
        if mapping.externalPartner
    ]
    for external_partner in external_partners:
        if external_partner and (
            org_id := get_wikidata_extracted_organization_id_by_name(external_partner)
        ):
            voxco_resource_organizations[external_partner] = org_id
    return voxco_resource_organizations


def extract_ldap_persons_voxco(
    voxco_resource_mappings: list[ResourceMapping],
) -> list[LDAPPerson]:
    """Extract LDAP persons for voxco.

    Args:
        voxco_resource_mappings: list of resource mapping models with default values

    Returns:
        list of LDAP persons
    """
    ldap = LDAPConnector.get()
    return [
        ldap.get_person(mail=mapping.contact[0].mappingRules[0].forValues[1])  # type: ignore[index]
        for mapping in voxco_resource_mappings
    ]

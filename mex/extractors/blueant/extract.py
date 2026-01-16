from collections.abc import Iterable

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPerson
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.blueant.connector import BlueAntConnector
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.logging import watch_progress
from mex.extractors.settings import Settings
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def extract_blueant_sources() -> list[BlueAntSource]:
    """Load Blue Ant sources from Blue Ant API.

    Returns:
        List of Blue Ant sources
    """
    connector = BlueAntConnector.get()

    sources = []
    for source in watch_progress(connector.get_projects(), "extract_blueant_sources"):
        department = connector.get_department_name(source.departmentId)
        type_ = connector.get_type_description(source.typeId)
        status = connector.get_status_name(source.statusId)
        project_leader_employee_id = connector.get_person(
            source.projectLeaderId
        ).personnelNumber
        client_names = [
            connector.get_client_name(client.clientId) for client in source.clients
        ]
        name = remove_prefixes_from_name(source.name)
        sources.append(
            BlueAntSource(
                client_names=client_names,
                department=department,
                end=source.end,
                name=name,
                number=source.number,
                projectLeaderEmployeeId=project_leader_employee_id,
                start=source.start,
                status=status,
                type_=type_,
            )
        )
    return sources


def extract_blueant_project_leaders(
    blueant_sources: Iterable[BlueAntSource],
) -> list[LDAPPerson]:
    """Extract LDAP persons for Blue Ant project leaders.

    Args:
        blueant_sources: Blue Ant sources

    Returns:
        List of LDAP persons
    """
    ldap = LDAPConnector.get()
    seen = set()
    persons = []
    for source in watch_progress(blueant_sources, "extract_blueant_project_leaders"):
        employee_id = source.projectLeaderEmployeeId
        if not employee_id:
            continue
        if employee_id in seen:
            continue
        seen.add(employee_id)
        try:
            persons.append(ldap.get_person(employee_id=employee_id))
        except MExError:
            continue
    return persons


def remove_prefixes_from_name(name: str) -> str:
    """Remove prefix according to settings.

    Args:
        name: string containing project label

    Settings:
        blueant.delete_prefixes: delete prefixes of labels starting with these terms

    Return:
        string cleaned of prefixes
    """
    settings = Settings.get()

    for prefix in settings.blueant.delete_prefixes:
        name = name.removeprefix(prefix)

    return name


def extract_blueant_organizations(
    blueant_sources: Iterable[BlueAntSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        blueant_sources: Iterable of blueant sources

    Returns:
        Dict with organization label and WikidataOrganization ID
    """
    merged_organization_ids_by_query_str: dict[str, MergedOrganizationIdentifier] = {}
    for source in blueant_sources:
        for name in source.client_names:
            if (
                not name
                or name in ["Robert Koch-Institut", "RKI"]
                or name in merged_organization_ids_by_query_str
            ):
                continue

            org_id = get_wikidata_extracted_organization_id_by_name(name)
            if not org_id:
                continue
            merged_organization_ids_by_query_str[name] = org_id

    return merged_organization_ids_by_query_str

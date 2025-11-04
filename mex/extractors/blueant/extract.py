from collections.abc import Generator, Iterable

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPerson
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.blueant.connector import BlueAntConnector
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.settings import Settings
from mex.extractors.utils import watch_progress
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def extract_blueant_sources() -> Generator[BlueAntSource, None, None]:
    """Load Blue Ant sources from Blue Ant API.

    Returns:
        Generator for Blue Ant sources
    """
    connector = BlueAntConnector.get()

    persons = connector.get_persons()
    blueant_id_to_employee_id_map = {p.id: p.personnelNumber for p in persons}

    for source in watch_progress(connector.get_projects(), "extract_blueant_sources"):
        department = connector.get_department_name(source.departmentId)
        type_ = connector.get_type_description(source.typeId)
        status = connector.get_status_name(source.statusId)
        project_leader_employee_id = blueant_id_to_employee_id_map.get(
            source.projectLeaderId
        )
        client_names = [
            connector.get_client_name(client.clientId) for client in source.clients
        ]
        name = remove_prefixes_from_name(source.name)
        yield BlueAntSource(
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


def extract_blueant_project_leaders(
    blueant_sources: Iterable[BlueAntSource],
) -> Generator[LDAPPerson, None, None]:
    """Extract LDAP persons for Blue Ant project leaders.

    Args:
        blueant_sources: Blue Ant sources

    Returns:
        Generator for LDAP persons
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in watch_progress(blueant_sources, "extract_blueant_project_leaders"):
        employee_id = source.projectLeaderEmployeeId
        if not employee_id:
            continue
        if employee_id in seen:
            continue
        seen.add(employee_id)
        try:
            yield ldap.get_person(employee_id=employee_id)
        except MExError:
            continue


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
    blueant_sources: list[BlueAntSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        blueant_sources: Iterable of blueant sources

    Returns:
        Dict with organization label and WikidataOrganization ID
    """
    return {
        name: org_id
        for source in blueant_sources
        for name in source.client_names
        if name not in ["Robert Koch-Institut", "RKI"]
        and (org_id := get_wikidata_extracted_organization_id_by_name(name))
    }

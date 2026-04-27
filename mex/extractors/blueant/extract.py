from typing import TYPE_CHECKING

from mex.extractors.blueant.connector import BlueAntConnector
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.ldap.helpers import get_ldap_merged_person_id_by_query
from mex.extractors.logging import watch_progress
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.common.types import MergedPersonIdentifier


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
) -> dict[str, MergedPersonIdentifier]:
    """Extract LDAP persons for Blue Ant project leaders.

    Args:
        blueant_sources: Blue Ant sources

    Returns:
        LDAP person ids by query
    """
    seen = set()
    person_ids_by_query: dict[str, MergedPersonIdentifier] = {}
    for source in watch_progress(blueant_sources, "extract_blueant_project_leaders"):
        employee_id = source.projectLeaderEmployeeId
        if not employee_id:
            continue
        if employee_id in seen:
            continue
        seen.add(employee_id)
        if person_id := get_ldap_merged_person_id_by_query(employee_id=employee_id):
            person_ids_by_query[employee_id] = person_id
    return person_ids_by_query


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

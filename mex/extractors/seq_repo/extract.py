from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.extractors.drop import DropApiConnector
from mex.extractors.ldap.helpers import get_ldap_merged_person_id_by_query
from mex.extractors.logging import watch_progress
from mex.extractors.seq_repo.model import SeqRepoSource

if TYPE_CHECKING:
    from mex.common.types import MergedPersonIdentifier


def extract_sources() -> list[SeqRepoSource]:
    """Extract Seq Repo sources by loading data from source json file.

    Returns:
        List of Seq Repo resources
    """
    connector = DropApiConnector.get()
    files = connector.list_files("seq-repo")
    if len(files) != 1:
        msg = f"Expected exactly one seq-repo file, got {len(files)}"
        raise MExError(msg)
    data = connector.get_file("seq-repo", files[0])
    return [SeqRepoSource.model_validate(item) for item in data]


def extract_source_project_coordinator_by_name(
    seq_repo_sources: dict[str, SeqRepoSource],
) -> dict[str, MergedPersonIdentifier]:
    """Extract LDAP persons with their query string for source project coordinators.

    Args:
        seq_repo_sources: Seq Repo sources

    Returns:
        List of LDAP persons with query
    """
    person_id_by_name: dict[str, MergedPersonIdentifier] = {}
    for value in watch_progress(
        seq_repo_sources.values(), "extract_source_project_coordinator_by_name"
    ):
        names = set(value.project_coordinators)
        for name in names:
            if person_id := get_ldap_merged_person_id_by_query(mail=f"{name}@rki.de"):
                person_id_by_name[name] = person_id
    return person_id_by_name

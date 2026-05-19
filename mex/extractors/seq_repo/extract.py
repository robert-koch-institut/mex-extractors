from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.extractors.drop import DropApiConnector
from mex.extractors.ldap.helpers import get_ldap_extracted_person_by_query
from mex.extractors.logging import watch_progress
from mex.extractors.seq_repo.model import SeqRepoSource

if TYPE_CHECKING:
    from mex.common.models import ExtractedPerson


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
    seq_repo_sources: list[SeqRepoSource],
) -> dict[str, ExtractedPerson]:
    """Extract Persons with their query string for source project coordinators.

    Args:
        seq_repo_sources: Seq Repo sources

    Returns:
        List of Extracted persons with query
    """
    person_by_name: dict[str, ExtractedPerson] = {}
    for source in watch_progress(
        seq_repo_sources, "extract_source_project_coordinator_by_name"
    ):
        names = set(source.project_coordinators)
        for name in names:
            if person := get_ldap_extracted_person_by_query(sam_account_name=name):
                person_by_name[name] = person
    return person_by_name

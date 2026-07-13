from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.extractors.drop import DropApiConnector
from mex.extractors.ldap.helpers import get_ldap_extracted_person_by_query
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
    """Extract Persons by their query string for source project coordinators.

    Args:
        seq_repo_sources: Seq Repo sources

    Returns:
        dictionary of Extracted persons by query
    """
    seen: set[str] = set()
    persons_with_query: dict[str, ExtractedPerson] = {}
    for source in seq_repo_sources:
        for name in source.project_coordinators:
            if name in seen:
                continue
            seen.add(name)
            if person := get_ldap_extracted_person_by_query(sam_account_name=name):
                persons_with_query[name] = person
    return persons_with_query

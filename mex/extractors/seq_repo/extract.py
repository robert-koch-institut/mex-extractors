from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.extractors.drop import DropApiConnector
from mex.extractors.logging import watch_progress
from mex.extractors.seq_repo.model import SeqRepoSource


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


def extract_source_project_coordinator(
    seq_repo_sources: dict[str, SeqRepoSource],
) -> list[LDAPPersonWithQuery]:
    """Extract LDAP persons with their query string for source project coordinators.

    Args:
        seq_repo_sources: Seq Repo sources

    Returns:
        List of LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    persons_with_query = []
    for value in watch_progress(
        seq_repo_sources.values(), "extract_source_project_coordinator"
    ):
        names = value.project_coordinators
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            persons = ldap.get_persons(mail=f"{name}@rki.de", limit=2)
            if len(persons) == 1 and persons[0].objectGUID:
                persons_with_query.append(
                    LDAPPersonWithQuery(person=persons[0], query=name)
                )
    return persons_with_query

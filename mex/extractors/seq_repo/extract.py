from collections.abc import Generator

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.logging import watch
from mex.extractors.drop import DropApiConnector
from mex.extractors.seq_repo.model import (
    SeqRepoSource,
)


def extract_sources() -> Generator[SeqRepoSource, None, None]:
    """Extract Seq Repo sources by loading data from source json file.

    Returns:
        Generator for Seq Repo resources
    """
    connector = DropApiConnector.get()
    files = connector.list_files("seq-repo")
    if len(files) != 1:
        msg = f"Expected exactly one seq-repo file, got {len(files)}"
        raise MExError(msg)
    data = connector.get_file("seq-repo", files[0])
    for item in data:
        yield SeqRepoSource.model_validate(item)


@watch()
def extract_source_project_coordinator(
    seq_repo_sources: dict[str, SeqRepoSource],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons with their query string for source project coordinators.

    Args:
        seq_repo_sources: Seq Repo sources

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    for value in seq_repo_sources.values():
        names = value.project_coordinators
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            persons = ldap.get_persons(mail=f"{name}@rki.de", limit=2)
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=name)

from collections.abc import Generator, Iterable

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPerson
from mex.common.logging import watch
from mex.extractors.rdmo.connector import RDMOConnector
from mex.extractors.rdmo.models.source import RDMOSource


@watch
def extract_rdmo_sources() -> Generator[RDMOSource, None, None]:
    """Load RDMO sources by querying the RDMO API.

    Returns:
        Generator for RDMO sources
    """
    rdmo = RDMOConnector.get()
    for source in rdmo.get_sources():
        source.question_answer_pairs = rdmo.get_question_answer_pairs(source.id)
        yield source


@watch
def extract_rdmo_source_contacts(
    rdmo_sources: Iterable[RDMOSource],
) -> Generator[LDAPPerson, None, None]:
    """Extract LDAP persons for RDMO owners.

    Args:
        rdmo_sources: RDMO sources

    Returns:
        Generator for LDAP persons
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in rdmo_sources:
        for owner in source.owners:
            if not owner.email:
                continue
            if owner.email in seen:
                continue
            seen.add(owner.email)
            try:
                yield ldap.get_person(mail=owner.email)
            except MExError:
                continue

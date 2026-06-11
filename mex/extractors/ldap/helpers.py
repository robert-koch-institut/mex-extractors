from functools import lru_cache
from typing import TYPE_CHECKING

from mex.common.exceptions import (
    EmptySearchResultError,
    FoundMoreThanOneError,
)
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_functional_account_to_extracted_contact_point,
    transform_ldap_person_to_extracted_person,
)
from mex.extractors.organigram.helpers import (
    get_extracted_organizational_units,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedPerson
    from mex.common.types import (
        MergedContactPointIdentifier,
        MergedPersonIdentifier,
    )


@lru_cache(maxsize=1024)
def get_ldap_extracted_person_by_query(  # noqa: PLR0913
    display_name: str = "*",
    employee_id: str = "*",
    given_name: str = "*",
    mail: str = "*",
    object_guid: str = "*",
    sam_account_name: str = "*",
    surname: str = "*",
) -> ExtractedPerson | None:
    """Extract, transform and load ldap person and return merged ID.

    Args:
        display_name: Display name of the person
        employee_id: Employee identifier
        given_name: Given name of a person, defaults to non-null
        mail: Email address, defaults to non-null
        object_guid: Internal LDAP identifier
        sam_account_name: Account name
        surname: Surname of a person, defaults to non-null

    Returns:
        merged person id or None, if not exactly one result
    """
    connector = LDAPConnector.get()
    try:
        ldap_person = connector.get_person(
            display_name=display_name,
            employee_id=employee_id,
            given_name=given_name,
            mail=mail,
            object_guid=object_guid,
            sam_account_name=sam_account_name,
            surname=surname,
        )
    except EmptySearchResultError, FoundMoreThanOneError:
        return None

    extracted_organizational_units = {
        unit.identifierInPrimarySource: unit
        for unit in get_extracted_organizational_units()
    }
    rki_organization_id = get_wikidata_extracted_organization_id_by_name("RKI")
    if not rki_organization_id:
        msg = "RKI wikidata organization not found"
        raise EmptySearchResultError(msg)

    extracted_person = transform_ldap_person_to_extracted_person(
        ldap_person,
        get_extracted_primary_source_id_by_name("ldap"),
        extracted_organizational_units,
        rki_organization_id,
    )
    load([extracted_person])
    return extracted_person


def get_ldap_merged_person_id_by_query(  # noqa: PLR0913
    display_name: str = "*",
    employee_id: str = "*",
    given_name: str = "*",
    mail: str = "*",
    object_guid: str = "*",
    sam_account_name: str = "*",
    surname: str = "*",
) -> MergedPersonIdentifier | None:
    """Extract, transform and load ldap person and return merged ID.

    Args:
        display_name: Display name of the person
        employee_id: Employee identifier
        given_name: Given name of a person, defaults to non-null
        mail: Email address, defaults to non-null
        object_guid: Internal LDAP identifier
        sam_account_name: Account name
        surname: Surname of a person, defaults to non-null

    Returns:
        merged person id or None, if not exactly one result
    """
    if extracted_person := get_ldap_extracted_person_by_query(
        display_name=display_name,
        employee_id=employee_id,
        given_name=given_name,
        mail=mail,
        object_guid=object_guid,
        sam_account_name=sam_account_name,
        surname=surname,
    ):
        return extracted_person.stableTargetId
    return None


@lru_cache(maxsize=1024)
def get_ldap_merged_contact_id_by_mail(
    *,
    mail: str = "*",
    limit: int = 10,
) -> MergedContactPointIdentifier | None:
    """Extract, transform and load ldap contact point and return merged ID.

    Args:
        mail: functional account mail
        limit: How many items to return

    Returns:
        merged contact point id or None, if not exactly one result
    """
    connector = LDAPConnector.get()
    functional_account = connector.get_functional_accounts(mail=mail, limit=limit).items
    if len(functional_account) != 1:
        return None
    extracted_contact_point = (
        transform_ldap_functional_account_to_extracted_contact_point(
            functional_account[0],
            get_extracted_primary_source_id_by_name("ldap"),
        )
    )
    load([extracted_contact_point])
    return extracted_contact_point.stableTargetId

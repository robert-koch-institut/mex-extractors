from mex.common.exceptions import EmptySearchResultError, FoundMoreThanOneError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_functional_account_to_extracted_contact_point,
    transform_ldap_person_and_unit_ids_to_extracted_person,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def get_ldap_merged_person_id_by_query(  # noqa: PLR0913
    person_unit_ids: list[MergedOrganizationalUnitIdentifier],
    *,
    display_name: str = "*",
    employee_id: str = "*",
    given_name: str = "*",
    mail: str = "*",
    object_guid: str = "*",
    sam_account_name: str = "*",
    surname: str = "*",
) -> MergedPersonIdentifier:
    """Extract, transform and load ldap person or contact and return merged ID.

    Args:
        person_unit_ids: merged unit ids
        display_name: Display name of the person
        employee_id: Employee identifier
        given_name: Given name of a person, defaults to non-null
        mail: Email address, defaults to non-null
        object_guid: Internal LDAP identifier
        sam_account_name: Account name
        surname: Surname of a person, defaults to non-null

    Raises:
        EmptySearchResultError if no result, FoundMoreThanOneError if multiple results

    Returns:
        merged person or contact point id
    """
    connector = LDAPConnector.get()
    ldap_person = connector.get_person(
        display_name=display_name,
        employee_id=employee_id,
        given_name=given_name,
        mail=mail,
        object_guid=object_guid,
        sam_account_name=sam_account_name,
        surname=surname,
    )
    rki_organization_id = get_wikidata_extracted_organization_id_by_name("RKI")
    if not rki_organization_id:
        msg = "Wikidata organization RKI not found."
        raise EmptySearchResultError(msg)
    extracted_person = transform_ldap_person_and_unit_ids_to_extracted_person(
        ldap_person,
        get_extracted_primary_source_id_by_name("ldap"),
        person_unit_ids,
        rki_organization_id,
    )
    load([extracted_person])
    return extracted_person.stableTargetId


def get_ldap_merged_contact_id_by_mail(
    *,
    mail: str = "*",
    limit: int = 10,
) -> MergedContactPointIdentifier:
    """Extract, transform and load ldap person or contact and return merged ID.

    Args:
        mail: person or functional account mail
        limit: How many items to return

    Raises:
        EmptySearchResultError if no result, FoundMoreThanOneError if multiple results

    Returns:
        merged person or contact point id
    """
    connector = LDAPConnector.get()
    functional_account = connector.get_functional_accounts(mail=mail, limit=limit)
    if len(functional_account) == 0:
        msg = f"No result for mail: {mail}"
        raise EmptySearchResultError(msg)
    if len(functional_account) > 1:
        msg = f"More than one result for mail: {mail}"
        raise FoundMoreThanOneError(msg)
    extracted_contact_point = (
        transform_ldap_functional_account_to_extracted_contact_point(
            functional_account[0],
            get_extracted_primary_source_id_by_name("ldap"),
        )
    )
    load([extracted_contact_point])
    return extracted_contact_point.stableTargetId

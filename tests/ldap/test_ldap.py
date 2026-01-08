from uuid import UUID

import pytest

from mex.common.ldap.connector import LDAPConnector

EXPECTED_PERSON = {
    "employeeID": "42",
    "sn": "Resolved",
    "givenName": ["Roland"],
    "displayName": "Resolved, Roland",
    "objectGUID": UUID("00000000-0000-4000-8000-000000000001"),
    "sAMAccountName": "test_person",
    "department": "PARENT-UNIT",
    "mail": ["test_person@email.de"],
    "company": None,
    "departmentNumber": None,
    "ou": [],
}


@pytest.mark.integration
def test_ldap_connector_get_person_by_employee_id() -> None:
    """Test fetching Roland Resolved by employee ID using LDAPConnector."""
    ldap = LDAPConnector.get()
    person = ldap.get_person(employee_id="42")

    assert person.model_dump() == EXPECTED_PERSON


@pytest.mark.integration
def test_ldap_connector_get_person_by_mail() -> None:
    """Test fetching Roland Resolved by email using LDAPConnector."""
    ldap = LDAPConnector.get()
    person = ldap.get_person(mail="test_person@email.de")

    assert person.model_dump() == EXPECTED_PERSON


@pytest.mark.integration
def test_ldap_connector_get_person_by_name() -> None:
    """Test fetching Roland Resolved by name using LDAPConnector."""
    ldap = LDAPConnector.get()
    person = ldap.get_person(given_name="Roland", surname="Resolved")

    assert person.model_dump() == EXPECTED_PERSON

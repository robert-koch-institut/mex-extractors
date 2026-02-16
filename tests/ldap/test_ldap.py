import pytest

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPerson


@pytest.mark.integration
@pytest.mark.usefixtures("mocked_ldap")
def test_ldap_connector_get_person_by_employee_id(
    ldap_roland_resolved: LDAPPerson,
) -> None:
    """Test fetching Roland Resolved by employee ID using LDAPConnector."""
    ldap = LDAPConnector.get()
    person = ldap.get_person(employee_id="42")

    assert person == ldap_roland_resolved


@pytest.mark.integration
@pytest.mark.usefixtures("mocked_ldap")
def test_ldap_connector_get_person_by_mail(
    ldap_roland_resolved: LDAPPerson,
) -> None:
    """Test fetching Roland Resolved by email using LDAPConnector."""
    ldap = LDAPConnector.get()
    person = ldap.get_person(mail="resolvedr@rki.de")

    assert person == ldap_roland_resolved


@pytest.mark.integration
@pytest.mark.usefixtures("mocked_ldap")
def test_ldap_connector_get_person_by_name(
    ldap_roland_resolved: LDAPPerson,
) -> None:
    """Test fetching Roland Resolved by name using LDAPConnector."""
    ldap = LDAPConnector.get()
    person = ldap.get_person(given_name="Roland", surname="Resolved")

    assert person == ldap_roland_resolved

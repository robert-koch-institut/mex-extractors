import pytest

from mex.common.ldap.connector import LDAPConnector


@pytest.mark.integration
def test_mock_available() -> None:
    ldap_connector = LDAPConnector.get()
    assert ldap_connector.get_persons(limit=42)
    assert not ldap_connector.get_persons(limit=42)

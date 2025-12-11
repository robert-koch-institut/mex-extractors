import pytest

from mex.common.ldap.connector import LDAPConnector

from mex.extractors.settings import Settings


@pytest.mark.integration
def test_mock_available() -> None:
    print(Settings.get().ldap_url.get_secret_value())
    print(Settings.get().ldap_url.get_secret_value()[3:9])
    ldap_connector = LDAPConnector.get()
    assert ldap_connector.get_persons(limit=42)
    assert not ldap_connector.get_persons(limit=42)

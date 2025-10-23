import pytest

from mex.extractors.consent_mailer.extract import (
    extract_consents_for_persons,
    extract_ldap_persons,
)


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
def test_extract_ldap_persons() -> None:
    persons = extract_ldap_persons()
    assert len(persons) == 2


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
def test_extract_consents_for_persons() -> None:
    # due to its mocked, we dont need any param
    persons = extract_ldap_persons()
    consents = extract_consents_for_persons(persons)
    assert len(consents) == 1

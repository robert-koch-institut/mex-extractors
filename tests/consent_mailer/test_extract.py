import pytest

from mex.common.models import ExtractedPrimarySource
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.consent_mailer.extract import (
    extract_consents_for_persons,
    extract_ldap_persons,
)


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
def test_extract_ldap_persons(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    persons = extract_ldap_persons(extracted_primary_sources["ldap"].stableTargetId)
    assert len(persons) == 2


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
def test_extract_consents_for_persons(
    mocked_consent_backend_api_connector: dict[str, int],
) -> None:
    persons = extract_ldap_persons(MergedPrimarySourceIdentifier(""))
    consents = extract_consents_for_persons(persons)
    assert len(consents) == 1
    # assert 3 calls: 1 for persons, 2 for consents (2 consents in batches of size 1)
    assert mocked_consent_backend_api_connector["count"] == 3

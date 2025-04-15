import time
from datetime import datetime

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_person_to_mex_person,
)
from mex.common.models import (
    ConsentMapping,
    ExtractedConsent,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.types import UTC
from mex.extractors.endnote.model import EndnoteRecord


def extract_endnote_persons(
    endnote_records: list[EndnoteRecord],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> list[ExtractedPerson]:
    """Extract endnote persons.

    Args:
        endnote_records: list of endnote record
        extracted_primary_source_ldap: primary Source for ldap
        extracted_organizational_units: list of organizational units

    Returns:
        list of extracted person
    """
    units_by_identifier_in_primary_source = {
        unit.identifierInPrimarySource: unit for unit in extracted_organizational_units
    }
    unique_persons = {author for record in endnote_records for author in record.authors}
    ldap = LDAPConnector.get()
    return [
        transform_ldap_person_to_mex_person(
            ldap.get_person(person),
            extracted_primary_source_ldap,
            units_by_identifier_in_primary_source,
        )
        for person in unique_persons
    ]


def extract_endnote_consents(
    extracted_endnote_persons: list[ExtractedPerson],
    extracted_primary_source_endnote: ExtractedPrimarySource,
    endnote_consent_mapping: ConsentMapping,
) -> list[ExtractedConsent]:
    """Extract endnote consent.

    Args:
        extracted_endnote_persons: list of endnote  persons
        extracted_primary_source_endnote: primary source for endnote
        endnote_consent_mapping: endnote consent mapping default values

    Returns:
        list of extracted consent
    """
    return [
        ExtractedConsent(
            hadPrimarySource=extracted_primary_source_endnote.stableTargetId,
            hasConsentStatus=endnote_consent_mapping.hasConsentStatus[0]
            .mappingRules[0]
            .setValues,
            hasConsentType=endnote_consent_mapping.hasConsentType[0]
            .mappingRules[0]
            .setValues,
            hasDataSubject=person.stableTargetId,
            identifierInPrimarySource=f"{person.stableTargetId}_consent",
            isIndicatedAtTime=datetime.fromtimestamp(time.time(), tz=UTC),
        )
        for person in extracted_endnote_persons
    ]

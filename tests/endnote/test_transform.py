import pytest
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.endnote.transform import (
    extract_endnote_bibliographic_resource,
    extract_endnote_consents,
    extract_endnote_persons_by_person_string,
)
from mex.common.models import (
    ExtractedPrimarySource,
    BibliographicResourceMapping,
    ExtractedPerson,
    ConsentMapping,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.common.testing import Joker


def test_extract_endnote_persons_by_person_string(
    endnote_records: list[EndnoteRecord],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    person_dict = extract_endnote_persons_by_person_string(
        endnote_records,
        extracted_primary_sources["endnote"],
    )
    assert person_dict["Mustermann, J."].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
        "identifierInPrimarySource": "Mustermann, J.",
        "familyName": ["Mustermann"],
        "givenName": ["J."],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_extract_endnote_consents(
    extracted_endnote_persons_by_person_string: dict[str, ExtractedPerson],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    endnote_consent_mapping: ConsentMapping,
) -> None:
    extracted_consents = extract_endnote_consents(
        list(extracted_endnote_persons_by_person_string.values()),
        extracted_primary_sources["endnote"],
        endnote_consent_mapping,
    )

    assert extracted_consents[0].model_dump() == {
        "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
        "identifierInPrimarySource": "ccSc9u7Kjps1nNBxTw7y3l_consent",
        "hasConsentStatus": "https://mex.rki.de/item/consent-status-2",
        "hasDataSubject": "ccSc9u7Kjps1nNBxTw7y3l",
        "isIndicatedAtTime": Joker(),
        "hasConsentType": "https://mex.rki.de/item/consent-type-1",
        "entityType": "ExtractedConsent",
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_extract_endnote_bibliographic_resource(
    endnote_records: list[EndnoteRecord],
    endnote_bibliographic_resource_mapping: BibliographicResourceMapping,
    extracted_endnote_persons_by_person_string: dict[str, ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    bibliographic_resources = extract_endnote_bibliographic_resource(
        endnote_records,
        endnote_bibliographic_resource_mapping,
        extracted_endnote_persons_by_person_string,
        unit_stable_target_ids_by_synonym,
        extracted_primary_sources["endnote"],
    )

    assert bibliographic_resources[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
        "identifierInPrimarySource": "1890-Converted.enl\\n1",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "doi": "https://doi.org/10.3456/qad.00",
        "edition": "1890/01/01",
        "issue": "6",
        "issued": "1890-01-01",
        "publicationYear": "1890",
        "repositoryURL": {"url": "https://www.rki.de"},
        "volume": "5",
        "creator": ["ccSc9u7Kjps1nNBxTw7y3l"],
        "title": [{"value": "test title", "language": "en"}],
        "abstract": [{"value": "abstract.", "language": "en"}],
        "bibliographicResourceType": [
            "https://mex.rki.de/item/bibliographic-resource-type-7"
        ],
        "contributingUnit": ["bFQoRhcVH5DHU8"],
        "editor": ["ccSc9u7Kjps1nNBxTw7y3l"],
        "editorOfSeries": ["ccSc9u7Kjps1nNBxTw7y3l"],
        "isbnIssn": ["1234-5678"],
        "journal": [{"value": "Journal Article ['full-title']", "language": "en"}],
        "keyword": [
            {"value": "keyword 1", "language": "en"},
            {"value": "keyword 2", "language": "en"},
        ],
        "language": ["https://mex.rki.de/item/language-2"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

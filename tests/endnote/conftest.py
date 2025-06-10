import pytest

from mex.common.models import (
    BibliographicResourceMapping,
    ConsentMapping,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def endnote_records() -> list[EndnoteRecord]:
    return [
        EndnoteRecord.model_validate(
            {
                "abstract": "abstract.",
                "authors": ["Mustermann, J.", "Mustermann,, K."],
                "call_num": "https://www.rki.de",
                "custom3": "Robert-Koch Institut",
                "custom4": "C1",
                "custom6": "Closed Access",
                "database": "1890-Converted.enl",
                "edition": "1890/01/01",
                "electronic_resource_num": "10.3456/qad.00",
                "isbn": "1234-5678",
                "keyword": ["keyword 1", "keyword 2"],
                "language": "eng",
                "number": "6",
                "pages": "3-4",
                "periodical": ["full-title"],
                "pub_dates": ["Jan 1", "Mar\rApr 1"],
                "publisher": "Robert-Koch Institut",
                "rec_number": "1",
                "ref_type": "Book Section",
                "related_urls": ["https://www.rki.de"],
                "secondary_authors": ["Erika Mustermann"],
                "secondary_title": "secondary test title",
                "tertiary_authors": ["Mustermann, I."],
                "title": "test title",
                "volume": "5",
                "year": "1890",
            }
        )
    ]


@pytest.fixture
def endnote_consent_mapping(settings: Settings) -> ConsentMapping:
    """Return endnote consent activity mapping from assets."""
    return ConsentMapping.model_validate(
        load_yaml(settings.endnote.mapping_path / "consent.yaml")
    )


@pytest.fixture
def endnote_bibliographic_resource_mapping(
    settings: Settings,
) -> BibliographicResourceMapping:
    """Return endnote bibliographic resource mapping from assets."""
    return BibliographicResourceMapping.model_validate(
        load_yaml(settings.endnote.mapping_path / "bibliographic-resource.yaml")
    )


@pytest.fixture
def extracted_endnote_persons_by_person_string(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> dict[str, ExtractedPerson]:
    return {
        "Mustermann, J.": ExtractedPerson.model_validate(
            {
                "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000001",
                "email": ["test_person@email.de"],
                "familyName": ["Resolved"],
                "fullName": ["Resolved, Roland"],
                "givenName": ["Roland"],
                "memberOf": ["hIiJpZXVppHvoyeP0QtAoS"],
            }
        ),
        "Erika Mustermann": ExtractedPerson.model_validate(
            {
                "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000002",
                "email": ["test_person@email.de"],
                "familyName": ["Secondary"],
                "fullName": ["Scondary, Roland"],
                "givenName": ["Roland"],
                "memberOf": ["hIiJpZXVppHvoyeP0QtAoS"],
            }
        ),
        "Mustermann, I.": ExtractedPerson.model_validate(
            {
                "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
                "identifierInPrimarySource": "00000000-0000-4000-8000-000000000003",
                "email": ["test_person@email.de"],
                "familyName": ["Tertiary"],
                "fullName": ["Tertiary, Roland"],
                "givenName": ["Roland"],
                "memberOf": ["hIiJpZXVppHvoyeP0QtAoS"],
            }
        ),
    }

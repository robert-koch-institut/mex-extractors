from datetime import datetime, tzinfo
from typing import TYPE_CHECKING, Self

import pytest
from pytest import MonkeyPatch

from mex.common.models import ExtractedOrganization
from mex.common.types import TextLanguage
from mex.extractors.endnote import transform
from mex.extractors.endnote.transform import (
    extract_endnote_bibliographic_resource,
    extract_endnote_consents,
    extract_endnote_persons_by_person_string,
    get_doi,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        BibliographicResourceMapping,
        ConsentMapping,
        ExtractedOrganizationalUnit,
        ExtractedPerson,
    )
    from mex.extractors.endnote.model import EndnoteRecord


def test_extract_endnote_persons_by_person_string(
    endnote_records: list[EndnoteRecord],
) -> None:
    person_dict = extract_endnote_persons_by_person_string(
        endnote_records,
    )
    assert sorted(person_dict.keys()) == [
        "Erika Mustermann",
        "Mustermann, I.",
        "Mustermann, J.",
    ]
    assert person_dict["Mustermann, J."].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("endnote"),
        "identifierInPrimarySource": "Person_Mustermann, J.",
        "familyName": ["Mustermann"],
        "fullName": ["Mustermann, J."],
        "givenName": ["J."],
        "identifier": "b6xSIhbOk5XL3FuXOiZCZl",
        "stableTargetId": "b2lkAngQwIPA4Yd6WAdz9b",
    }


def test_extract_endnote_consents(
    monkeypatch: MonkeyPatch,
    endnote_extracted_persons_by_person_str: dict[str, ExtractedPerson],
    endnote_consent_mapping: ConsentMapping,
) -> None:
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> Self:
            return cls(2024, 1, 1, 12, tzinfo=tz)

    monkeypatch.setattr(transform, "datetime", FrozenDatetime)
    extracted_consents = extract_endnote_consents(
        list(endnote_extracted_persons_by_person_str.values()),
        endnote_consent_mapping,
    )

    assert extracted_consents[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("endnote")),
        "identifierInPrimarySource": (
            f"{endnote_extracted_persons_by_person_str['Mustermann, J.'].stableTargetId}_consent"
        ),
        "hasConsentStatus": "https://mex.rki.de/item/consent-status-2",
        "hasDataSubject": endnote_extracted_persons_by_person_str[
            "Mustermann, J."
        ].stableTargetId,
        "isIndicatedAtTime": "2024-01-01T12:00:00Z",
        "identifier": "L2W9S9zDljX50xToC91SA",
        "stableTargetId": "bzNWxMPcrMBc0OlgYGUPg1",
    }


@pytest.mark.parametrize(
    ("electronic_resource_num", "expected_doi"),
    [
        ("https://edoc.rki.de/handle/abc", None),
        ("https://doi.org/10.3456/qad.00", "https://doi.org/10.3456/qad.00"),
        ("10.3456/qad.00", "https://doi.org/10.3456/qad.00"),
        ("abcdef", None),
        (None, None),
    ],
)
def test_get_doi(
    electronic_resource_num: str | None,
    expected_doi: str | None,
    endnote_bibliographic_resource_mapping: BibliographicResourceMapping,
) -> None:
    doi = get_doi(electronic_resource_num, endnote_bibliographic_resource_mapping)

    assert doi == expected_doi


@pytest.mark.usefixtures("mocked_wikidata")
def test_extract_endnote_bibliographic_resource(
    endnote_records: list[EndnoteRecord],
    endnote_bibliographic_resource_mapping: BibliographicResourceMapping,
    endnote_extracted_persons_by_person_str: dict[str, ExtractedPerson],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    bibliographic_resources = extract_endnote_bibliographic_resource(
        endnote_records,
        endnote_bibliographic_resource_mapping,
        endnote_extracted_persons_by_person_str,
    )
    rki_publisher = ExtractedOrganization(
        hadPrimarySource=get_extracted_primary_source_id_by_name("endnote"),
        identifierInPrimarySource="Organization_Robert-Koch Institut",
        officialName=["Robert-Koch Institut"],
    )

    assert bibliographic_resources[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("endnote")),
        "identifierInPrimarySource": "1890-Converted.enl::1",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "doi": "https://doi.org/10.3456/qad.00",
        "issue": "6",
        "issued": "1890-01-01",
        "publicationYear": "1890",
        "repositoryURL": [{"url": "https://www.rki.de"}],
        "volume": "5",
        "creator": [
            endnote_extracted_persons_by_person_str["Mustermann, J."].stableTargetId
        ],
        "title": [{"value": "test title", "language": TextLanguage.EN}],
        "titleOfSeries": [
            {"value": "full-title", "language": TextLanguage.EN},
            {"value": "secondary test title", "language": TextLanguage.EN},
        ],
        "abstract": [{"value": "abstract.", "language": TextLanguage.EN}],
        "bibliographicResourceType": [
            "https://mex.rki.de/item/bibliographic-resource-type-2"
        ],
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "editor": [
            endnote_extracted_persons_by_person_str["Erika Mustermann"].stableTargetId
        ],
        "editorOfSeries": [
            endnote_extracted_persons_by_person_str["Mustermann, I."].stableTargetId
        ],
        "isbnIssn": ["1234-5678"],
        "keyword": [
            {"value": "keyword 1", "language": TextLanguage.EN},
            {"value": "keyword 2", "language": TextLanguage.EN},
        ],
        "language": ["https://mex.rki.de/item/language-2"],
        "publisher": [rki_publisher.stableTargetId, rki_publisher.stableTargetId],
        "identifier": "cpbH9mYS20S3BlnCNgj17d",
        "stableTargetId": "djPDWpMKpTUaSRIfKgYdN6",
    }

import pytest

from mex.common.models import (
    BibliographicResourceMapping,
    ConsentMapping,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.testing import Joker
from mex.common.types import MergedOrganizationalUnitIdentifier, TextLanguage
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.endnote.transform import (
    extract_endnote_bibliographic_resource,
    extract_endnote_consents,
    extract_endnote_persons_by_person_string,
    get_doi,
)


def test_extract_endnote_persons_by_person_string(
    endnote_records: list[EndnoteRecord],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    person_dict = extract_endnote_persons_by_person_string(
        endnote_records,
        extracted_primary_sources["endnote"],
    )
    assert sorted(person_dict.keys()) == [
        "Erika Mustermann",
        "Mustermann, I.",
        "Mustermann, J.",
    ]
    assert person_dict["Mustermann, J."].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": extracted_primary_sources["endnote"].stableTargetId,
        "identifierInPrimarySource": "Person_Mustermann, J.",
        "familyName": ["Mustermann"],
        "fullName": ["Mustermann, J."],
        "givenName": ["J."],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_extract_endnote_consents(
    endnote_extracted_persons_by_person_string: dict[str, ExtractedPerson],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    endnote_consent_mapping: ConsentMapping,
) -> None:
    extracted_consents = extract_endnote_consents(
        list(endnote_extracted_persons_by_person_string.values()),
        extracted_primary_sources["endnote"],
        endnote_consent_mapping,
    )

    assert extracted_consents[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": str(extracted_primary_sources["endnote"].stableTargetId),
        "identifierInPrimarySource": "ccSc9u7Kjps1nNBxTw7y3l_consent",
        "hasConsentStatus": "https://mex.rki.de/item/consent-status-2",
        "hasDataSubject": "ccSc9u7Kjps1nNBxTw7y3l",
        "isIndicatedAtTime": Joker(),
        "identifier": Joker(),
        "stableTargetId": Joker(),
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


def test_extract_endnote_bibliographic_resource(
    endnote_records: list[EndnoteRecord],
    endnote_bibliographic_resource_mapping: BibliographicResourceMapping,
    endnote_extracted_persons_by_person_string: dict[str, ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    bibliographic_resources = extract_endnote_bibliographic_resource(
        endnote_records,
        endnote_bibliographic_resource_mapping,
        endnote_extracted_persons_by_person_string,
        unit_stable_target_ids_by_synonym,
        extracted_primary_sources["endnote"],
    )

    assert bibliographic_resources[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": str(extracted_primary_sources["endnote"].stableTargetId),
        "identifierInPrimarySource": "1890-Converted.enl::1",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "doi": "https://doi.org/10.3456/qad.00",
        "issue": "6",
        "issued": "1890-01-01",
        "publicationYear": "1890",
        "repositoryURL": [{"url": "https://www.rki.de"}],
        "volume": "5",
        "creator": ["ccSc9u7Kjps1nNBxTw7y3l"],
        "title": [{"value": "test title", "language": TextLanguage.EN}],
        "titleOfSeries": [
            {"value": "full-title", "language": TextLanguage.EN},
            {"value": "secondary test title", "language": TextLanguage.EN},
        ],
        "abstract": [{"value": "abstract.", "language": TextLanguage.EN}],
        "bibliographicResourceType": [
            "https://mex.rki.de/item/bibliographic-resource-type-2"
        ],
        "contributingUnit": ["bFQoRhcVH5DHU8"],
        "editor": ["eAoOVRo8MGXiVcaDyJwoaf"],
        "editorOfSeries": ["c4Mgoj3j2OtABRXefB9vAy"],
        "isbnIssn": ["1234-5678"],
        "keyword": [
            {"value": "keyword 1", "language": TextLanguage.EN},
            {"value": "keyword 2", "language": TextLanguage.EN},
        ],
        "language": ["https://mex.rki.de/item/language-2"],
        "publisher": ["dKesJpFog76YEZ4BhPwKuF", "dKesJpFog76YEZ4BhPwKuF"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

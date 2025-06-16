import pytest

from mex.extractors.endnote.extract import extract_endnote_records


@pytest.mark.usefixtures("mocked_drop", "mocked_wikidata")
def test_extract_endnote_records() -> None:
    records = extract_endnote_records()
    expected = {
        "abstract": "abstract.",
        "authors": ["Mustermann, E.", "Mustermann, F."],
        "call_num": "https://edoc.rki.de/handle/1234",
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
        "pub_dates": ["Jan 1"],
        "publisher": "Robert-Koch Institut",
        "rec_number": "1",
        "ref_type": "Journal Article",
        "related_urls": ["https://www.rki.de"],
        "secondary_authors": ["Mustermann, G.", "Mustermann, H."],
        "secondary_title": "secondary test title",
        "tertiary_authors": ["Mustermann, I.", "Mustermann, J."],
        "title": "test title",
        "volume": "5",
        "year": "1890",
    }
    assert records[0].model_dump() == expected

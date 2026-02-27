import pytest

from mex.common.models import MergedActivity
from mex.common.types import LinkLanguage, TextLanguage
from mex.extractors.datenkompass.extract import (
    get_extracted_item_stable_target_ids,
    get_filtered_primary_source_ids,
    get_merged_items,
)


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_get_merged_items_mocked() -> None:
    items = get_merged_items(entity_type=["MergedActivity"])
    assert len(items) == 3  # 3 mocked MergedActivites
    assert isinstance(items[1], MergedActivity)
    assert items[0].model_dump(exclude_defaults=True) == {
        "contact": ["LoremIpsum1234"],
        "responsibleUnit": ["IdentifierUnitC1", "IdentifierUnitFG99"],
        "title": [
            {"value": '"title "Act" no language"'},
            {"value": "title en", "language": TextLanguage.EN},
        ],
        "abstract": [
            {"value": "Die Nutzung", "language": TextLanguage.DE},
            {"value": "The usage", "language": TextLanguage.EN},
        ],
        "end": ["2025-02-25"],
        "funderOrCommissioner": ["Identifier2forORG", "NoORGIdentifier"],
        "shortName": [
            {"value": "short en", "language": TextLanguage.EN},
            {"value": "short de", "language": TextLanguage.DE},
        ],
        "start": ["2021-02-21"],
        "theme": ["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ...
        "website": [
            {"url": "https://www.dont-transform.de"},
            {
                "language": LinkLanguage.DE,
                "title": "Ja",
                "url": "https://www.do-transform.org",
            },
        ],
        "identifier": "MergedActivityWithORG2",
    }


@pytest.mark.usefixtures("mocked_backend_datenkompass", "mocked_provider")
def test_get_extracted_item_stable_target_ids() -> None:
    result = get_extracted_item_stable_target_ids(
        ["ExtractedResource"], ["thisdoesnotmatter"]
    )
    assert result == ["IdMergedWithExtracted"]


@pytest.mark.usefixtures("mocked_backend_datenkompass", "mocked_provider")
def test_get_filtered_primary_source_ids_mocked() -> None:
    result = get_filtered_primary_source_ids(["relevant primary source"])

    assert len(result) == 1
    assert result[0] == "identifierRelevantPS"

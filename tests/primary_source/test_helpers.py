from unittest.mock import Mock

from pytest import MonkeyPatch

from mex.common.models import ExtractedPrimarySource
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.primary_source import helpers
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
    load_extracted_primary_source_by_name,
)


def test_load_extracted_primary_source_by_name(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    monkeypatch: MonkeyPatch,
) -> None:
    """Helper finds "Wikidata" and loads, returns None for nonsense query."""
    query_wiki = "wikidata"
    query_nonsense = "this should give None"

    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    returned = load_extracted_primary_source_by_name(query_wiki)
    mocked_load.assert_called_once()

    expected = extracted_primary_sources["wikidata"]

    assert returned == expected
    assert load_extracted_primary_source_by_name(query_nonsense) is None


def test_get_extracted_primary_source_id_by_name() -> None:
    """Helper finds "Wikidata" and returns its ID, returns None for nonsense query."""
    query_wiki = "wikidata"
    query_nonsense = "this should give None"

    assert get_extracted_primary_source_id_by_name(
        query_wiki
    ) == MergedPrimarySourceIdentifier("djbNGb5fLgYHFyMh3fZE2g")
    assert get_extracted_primary_source_id_by_name(query_nonsense) is None

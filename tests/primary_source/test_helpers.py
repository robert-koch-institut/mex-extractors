from unittest.mock import Mock

from pytest import MonkeyPatch, raises

from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.primary_source import helpers
from mex.extractors.primary_source.helpers import (
    cached_load_extracted_primary_source_by_name,
    get_extracted_primary_source_id_by_name,
)


def test_cached_load_extracted_primary_source_by_name(
    monkeypatch: MonkeyPatch,
) -> None:
    """Helper finds "Wikidata" and loads, returns None for nonsense query."""
    query_wiki = "wikidata"
    query_nonsense = "this should give None"

    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    returned = cached_load_extracted_primary_source_by_name(query_wiki)
    mocked_load.assert_called_once()
    assert returned
    assert returned.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "00000000000000",
        "identifierInPrimarySource": "wikidata",
        "identifier": "gNrpqARBAlwazXAva81Tuq",
        "stableTargetId": "djbNGb5fLgYHFyMh3fZE2g",
    }
    assert cached_load_extracted_primary_source_by_name(query_nonsense) is None

    returned = cached_load_extracted_primary_source_by_name(query_wiki)
    mocked_load.assert_called_once()  # no additional calling cause query_wiki is cached


def test_get_extracted_primary_source_id_by_name() -> None:
    """Helper finds "Wikidata" and returns its ID, returns None for nonsense query."""
    query_wiki = "wikidata"
    query_nonsense = "this should raise an error"

    assert get_extracted_primary_source_id_by_name(
        query_wiki
    ) == MergedPrimarySourceIdentifier("djbNGb5fLgYHFyMh3fZE2g")

    with raises(
        NameError, match=r"Primary source name this should raise an error not found."
    ):
        get_extracted_primary_source_id_by_name(query_nonsense)

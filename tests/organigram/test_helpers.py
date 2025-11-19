from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
)
from mex.extractors.organigram import helpers
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym


@pytest.mark.usefixtures("mocked_wikidata")
def test_get_unit_merged_id_by_synonym(monkeypatch: MonkeyPatch) -> None:
    # Mock the load function to avoid actual sink writes
    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    # Create a mock organizational unit identifier
    mock_unit_id = MergedOrganizationalUnitIdentifier.generate(seed=1234599)

    # Mock get_extracted_unit_by_synonyms to return our mock unit
    mocked_get_unit = Mock(return_value={"C1": mock_unit_id})
    monkeypatch.setattr(helpers, "get_unit_merged_ids_by_synonyms", mocked_get_unit)

    # Test successful lookup
    merged_id = get_unit_merged_id_by_synonym("C1")

    # Verify load was called with the unit
    mocked_load.assert_called_once()

    # Verify we get back a merged ID
    assert isinstance(merged_id, MergedOrganizationalUnitIdentifier)
    assert merged_id == mock_unit_id


@pytest.mark.usefixtures("mocked_wikidata")
def test_get_unit_merged_id_by_synonym_not_found(monkeypatch: MonkeyPatch) -> None:
    # Mock the load function
    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    # Mock get_extracted_unit_by_synonyms to return empty dict
    mocked_get_unit = Mock(return_value={})
    monkeypatch.setattr(helpers, "get_unit_merged_ids_by_synonyms", mocked_get_unit)

    # Test lookup with unknown synonym
    merged_id = get_unit_merged_id_by_synonym("UNKNOWN")

    # Verify load was not called
    mocked_load.assert_not_called()

    # Verify we get None back
    assert merged_id is None

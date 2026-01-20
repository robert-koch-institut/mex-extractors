from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
)
from mex.extractors.organigram import helpers
from mex.extractors.organigram.helpers import (
    get_unit_merged_id_by_synonym,
    resolve_organizational_unit_with_fallback,
)


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

    # Set-up another Mock to test for caching and unknown synonym
    # Mock the load function
    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    # Mock get_extracted_unit_by_synonyms to return empty dict
    mocked_get_unit = Mock(return_value={})
    monkeypatch.setattr(helpers, "get_unit_merged_ids_by_synonyms", mocked_get_unit)

    # Test second lookup with unknown synonym
    merged_id = get_unit_merged_id_by_synonym("UNKNOWN")

    # Verify load was not called since function is cached
    mocked_load.assert_not_called()

    # Verify we get None back
    assert merged_id is None


@pytest.mark.parametrize(
    ("extracted_unit", "contact_ids", "involved_ids", "expected_unit"),
    [
        ("DIRECT_UNIT", [], [], "DIRECT_UNIT"),
        ("OUTDATED_UNIT", ["contact-1"], [], "LDAP_UNIT_CONTACT"),
        ("OUTDATED_UNIT", [], ["involved-1"], "LDAP_UNIT_INVOLVED"),
    ],
)
def test_resolve_organizational_unit_with_fallback_param(
    monkeypatch: MonkeyPatch,
    extracted_unit: str,
    contact_ids: list[str],
    involved_ids: list[str],
    expected_unit: str,
) -> None:
    merged_ids = {name: [MergedOrganizationalUnitIdentifier.generate(seed=i + 1)] 
                  for i, name in enumerate(
                      ["DIRECT_UNIT", "LDAP_UNIT_CONTACT", "LDAP_UNIT_INVOLVED"]
                  )}

    def _mock_get_cached_unit_merged_ids_by_synonyms()-> dict[str, list[MergedOrganizationalUnitIdentifier]]:
        return merged_ids

    monkeypatch.setattr(
        helpers,
        "_get_cached_unit_merged_ids_by_synonyms",
        _mock_get_cached_unit_merged_ids_by_synonyms,
    )

    def _mock_get_ldap_units_for_employee_ids(employee_ids: set[str])-> set[str]:
        if employee_ids == {"contact-1"}:
            return {"LDAP_UNIT_CONTACT"}
        if employee_ids == {"involved-1"}:
            return {"LDAP_UNIT_INVOLVED"}
        return set()

    monkeypatch.setattr(
        helpers,
        "get_ldap_units_for_employee_ids",
        _mock_get_ldap_units_for_employee_ids,
    )

    result = resolve_organizational_unit_with_fallback(
        extracted_unit=extracted_unit,
        contact_employee_ids=contact_ids,
        involved_employee_ids=involved_ids,
    )

    assert result == merged_ids[expected_unit]

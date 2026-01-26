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
    ("extracted_unit", "employee_ids", "expected_unit"),
    [
        ("Space Rocket", [], "Space Rocket"),
        ("Starship OLD", ["contact-1"], "Starship"),
        ("Starship OLD 2", ["involved-1"], "Starship 2"),
    ],
    ids=["unit matches organigramm", "unit old, fallback", "unit old, fallback"],
)
def test_resolve_organizational_unit_with_fallback_param(
    monkeypatch: MonkeyPatch,
    extracted_unit: str,
    employee_ids: list[str],
    expected_unit: str,
) -> None:
    mocked_unit_id_rocket = MergedOrganizationalUnitIdentifier.generate(seed=42)
    mocked_unit_id_starship = MergedOrganizationalUnitIdentifier.generate(seed=42)

    mocked_synonyms = {
        "Space Rocket": [mocked_unit_id_rocket],
        "Starship": [mocked_unit_id_starship],
        "Starship 2": [mocked_unit_id_starship],
    }

    monkeypatch.setattr(
        helpers,
        "_get_cached_unit_merged_ids_by_synonyms",
        lambda: mocked_synonyms,
    )

    def _mock_get_ldap_units_for_employee_ids(employee_ids: set[str]) -> set[str]:
        if employee_ids == {"contact-1"}:
            return {"Starship"}
        if employee_ids == {"involved-1"}:
            return {"Starship 2"}
        return set()

    monkeypatch.setattr(
        helpers,
        "get_ldap_units_for_employee_ids",
        _mock_get_ldap_units_for_employee_ids,
    )

    result = resolve_organizational_unit_with_fallback(
        extracted_unit=extracted_unit,
        contact_ids=employee_ids,
    )

    assert result == mocked_synonyms[expected_unit]

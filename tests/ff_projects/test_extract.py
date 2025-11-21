from datetime import datetime
from unittest.mock import MagicMock

import pytest

from mex.common.types import MergedOrganizationIdentifier, TemporalEntity
from mex.extractors.ff_projects.extract import (
    extract_ff_projects_organizations,
    extract_ff_projects_sources,
    get_clean_names,
    get_optional_string_from_cell,
    get_string_from_cell,
    get_temporal_entity_from_cell,
)


def test_extract_ff_projects_sources() -> None:
    sources = list(extract_ff_projects_sources())
    source_dicts = [s.model_dump(exclude_none=True) for s in sources]

    expected = [
        {
            "kategorie": "Entgelt",
            "thema_des_projekts": "Skipped Auftraggeber",
            "rki_az": "1364",
            "laufzeit_cells": (None, None),
            "projektleiter": "(Leitung) 1 Ficticious, Frieda / OE1?",
            "rki_oe": "Department",
            "zuwendungs_oder_auftraggeber": "Sonstige",
            "lfd_nr": "17",
        },
        {
            "kategorie": "Auftragsforschung",
            "foerderprogr": "Funding",
            "thema_des_projekts": "Fully Specified Source",
            "rki_az": "1364",
            "laufzeit_cells": ("2018-01-01 00:00:00", "2019-09-01 00:00:00"),
            "laufzeit_bis": "2019-08-31T23:42:00Z",
            "laufzeit_von": "2017-12-31T23:42:00Z",
            "projektleiter": "Dr Frieda Ficticious",
            "rki_oe": "Department",
            "zuwendungs_oder_auftraggeber": "Test-Institute",
            "lfd_nr": "18",
        },
    ]

    assert source_dicts[8:10] == expected


@pytest.mark.parametrize(
    ("name", "expected_clean_name"),
    [
        ("The-Alpha 2) Centuri", "The Alpha / Centuri"),
        ("Centuri ???", "Centuri"),
        ("The Alpha / Centuri B", "The Alpha / Centuri B"),
        ("The Alpha (Centuri)", "The Alpha /Centuri"),
        ("The Alpha-Centuri", "The Alpha Centuri"),
    ],
)
def test_get_clean_names(name: str, expected_clean_name: str) -> None:
    clean_name = get_clean_names(name)

    assert clean_name == expected_clean_name


def test_get_temporal_entity_from_cell() -> None:
    cell_value = datetime(2018, 1, 1, 0, 0)  # noqa: DTZ001
    ts = get_temporal_entity_from_cell(cell_value)
    expected_ts = TemporalEntity("2017-12-31T23:42:00Z")
    assert ts == expected_ts

    cell_value = MagicMock()
    ts = get_temporal_entity_from_cell(cell_value)
    assert ts is None


@pytest.mark.parametrize(
    ("cell_value", "expected"), [("2004 ", "2004"), (2004, "2004"), (" ", " ")]
)
def test_get_string_from_cell(cell_value: str | int, expected: str) -> None:
    string = get_string_from_cell(cell_value)
    assert string == expected


def test_get_optional_string_from_cell() -> None:
    cell_value = "2004"
    string = get_optional_string_from_cell(cell_value)
    assert string == "2004"

    cell_value = ""
    string = get_optional_string_from_cell(cell_value)
    assert string is None


@pytest.mark.usefixtures("mocked_wikidata")
def test_extract_ff_projects_organizations() -> None:
    sources = extract_ff_projects_sources()
    organizations = extract_ff_projects_organizations([sources[0]])
    assert organizations["Robert Koch-Institut"] == MergedOrganizationIdentifier(
        "ga6xh6pgMwgq7DC7r6Wjqg"
    )

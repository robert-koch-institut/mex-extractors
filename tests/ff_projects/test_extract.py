from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from mex.common.types import TemporalEntity, TemporalEntityPrecision
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.ff_projects.extract import (
    extract_ff_projects_organizations,
    extract_ff_projects_sources,
    filter_out_duplicate_source_ids,
    get_clean_names,
    get_optional_string_from_cell,
    get_string_from_cell,
    get_timestamp_from_cell,
)


def test_extract_ff_projects_sources() -> None:
    sources = list(extract_ff_projects_sources())
    source_dicts = [s.model_dump(exclude_none=True) for s in sources]

    expected = [
        {
            "kategorie": "Sonstige",
            "thema_des_projekts": "Skipped Kategorie",
            "rki_az": "1364",
            "laufzeit_cells": (None, None),
            "projektleiter": "Dr. Ficticious, Frieda",
            "zuwendungs_oder_auftraggeber": "Banana",
            "lfd_nr": "10",
        },
        {
            "kategorie": "Auftragsforschung",
            "foerderprogr": "Sonstige",
            "thema_des_projekts": "Skipped Förderprogr.",
            "rki_az": "1364",
            "laufzeit_cells": (None, None),
            "projektleiter": "Dr Frieda Ficticious",
            "zuwendungs_oder_auftraggeber": "Orange",
            "lfd_nr": "11",
        },
    ]

    assert source_dicts[1:3] == expected


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


def test_get_timestamp_from_cell() -> None:
    cell_value = datetime(2018, 1, 1, 0, 0)
    ts = get_timestamp_from_cell(cell_value)
    expected_ts = TemporalEntity("2017-12-31T23:00:00Z")
    expected_ts.precision = TemporalEntityPrecision.DAY
    assert ts == expected_ts

    cell_value = MagicMock()
    ts = get_timestamp_from_cell(cell_value)
    assert ts is None


@pytest.mark.parametrize(
    ("cell_value", "expected"), [("2004 ", "2004"), (2004, "2004"), (" ", " ")]
)
def test_get_string_from_cell(cell_value: Any, expected: str) -> None:
    string = get_string_from_cell(cell_value)
    assert string == expected


def test_get_optional_string_from_cell() -> None:
    cell_value = "2004"
    string = get_optional_string_from_cell(cell_value)
    assert string == "2004"

    cell_value = ""
    string = get_optional_string_from_cell(cell_value)
    assert string is None


def test_filter_out_duplicate_source_ids() -> None:
    ff_proj_liste_sources = list(extract_ff_projects_sources())

    assert len(ff_proj_liste_sources) == 21
    unfiltered_ids = [s.lfd_nr for s in ff_proj_liste_sources]
    assert unfiltered_ids.count("20") == 2

    filtered = list(filter_out_duplicate_source_ids(ff_proj_liste_sources))

    assert len(filtered) == 19
    filtered_ids = [s.lfd_nr for s in filtered]
    assert filtered_ids.count("20") == 0


@pytest.mark.usefixtures(
    "mocked_wikidata",
)
def test_extract_ff_projects_organizations(
    wikidata_organization: WikidataOrganization,
) -> None:
    organizations = extract_ff_projects_organizations(
        [next(extract_ff_projects_sources())]
    )
    assert organizations == {
        "Apple": wikidata_organization,
    }

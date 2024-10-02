from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.confluence_vvt import main
from mex.extractors.confluence_vvt.models.source import ConfluenceVvtSource


@pytest.fixture
def mocked_confluence_vvt(
    monkeypatch: MonkeyPatch,
    confluence_vvt_sources_dict: dict[str, ConfluenceVvtSource],
) -> None:
    """Mock the Confluence-vvt extractor to return mocked data."""
    monkeypatch.setattr(
        main,
        "fetch_all_data_page_ids",
        MagicMock(return_value=list(confluence_vvt_sources_dict)),
    )
    monkeypatch.setattr(
        main,
        "fetch_all_pages_data",
        MagicMock(return_value=confluence_vvt_sources_dict.values()),
    )


@pytest.fixture
def confluence_vvt_sources_dict() -> dict[str, ConfluenceVvtSource]:
    return {
        "fake-1": ConfluenceVvtSource(
            abstract="test description, test test test, test zwecke des vorhabens",
            contact=["Test Verantwortliche 1"],
            identifier="fake-2",
            identifier_in_primary_source=["001-002"],
            involved_person=[
                "Test Verantwortliche 1",
                "test ggfs vertreter",
                "Test mitarbeitende 1",
            ],
            involved_unit=["Test OE 1", "FG99", "test OE 1"],
            responsible_unit=["Test OE 1"],
            theme="https://mex.rki.de/item/theme-1",
            title="Test Title",
        ),
        "fake-2": ConfluenceVvtSource(
            abstract="test description, test test test, test zwecke des vorhabens",
            contact=["Test Verantwortliche 1"],
            identifier="fake-2",
            identifier_in_primary_source=["2022-006"],
            involved_person=[
                "Test Verantwortliche 1",
                "test ggfs vertreter",
                "Test mitarbeitende 1",
            ],
            involved_unit=["Test OE 1", "FG99", "test OE 1"],
            responsible_unit=["Test OE 1"],
            theme="https://mex.rki.de/item/theme-1",
            title="Test Title",
        ),
    }

import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests.models import Response

from mex.common.models import ActivityMapping, ExtractedPrimarySource
from mex.common.models.organization import ExtractedOrganization
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture
def unit_merged_ids_by_synonym(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    rki_organization: ExtractedOrganization,
) -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Return unit merged ids by synonym for organigram units."""
    organigram_units = extract_organigram_units()
    mex_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units, extracted_primary_sources["organigram"], rki_organization
    )
    return get_unit_merged_ids_by_synonyms(mex_organizational_units)


@pytest.fixture
def detail_page_data_html() -> str:
    """Return dummy detail page HTML."""
    with (TEST_DATA_DIR / "detail_page_data.html").open(encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture
def detail_page_data_json(detail_page_data_html: str) -> dict[str, Any]:
    """Return dummy detail page JSON."""
    with (TEST_DATA_DIR / "detail_page_data.json").open(encoding="utf-8") as fh:
        detail_page = json.load(fh)
    detail_page["body"]["view"]["value"] = detail_page_data_html
    return cast("dict[str, Any]", detail_page)


@pytest.fixture
def mocked_confluence_vvt_detailed_page_data(
    monkeypatch: MonkeyPatch, detail_page_data_json: dict[str, Any]
) -> None:
    """Mock the Confluence-vvt connector to return dummy data of the details page."""
    response = Mock(spec=Response, status_code=200)
    response.json.return_value = detail_page_data_json

    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response])

    monkeypatch.setattr(
        ConfluenceVvtConnector,
        "__init__",
        lambda self, _: setattr(self, "session", session),
    )


@pytest.fixture
def confluence_vvt_activity_mapping(settings: Settings) -> ActivityMapping:
    """Return confluence-vvt activity mapping from assets."""
    return ActivityMapping.model_validate(
        load_yaml(settings.confluence_vvt.template_v1_mapping_path / "activity.yaml")
    )

from itertools import tee
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests.models import Response

from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.models import ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import Identifier, TextLanguage
from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.extract import (
    extract_confluence_vvt_authors,
    fetch_all_data_page_ids,
    fetch_all_pages_data,
)
from mex.extractors.confluence_vvt.transform import (
    transform_confluence_vvt_sources_to_extracted_activities,
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.mark.integration
def test_transform_confluence_vvt_source_items_to_mex_activity(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, Identifier],
) -> None:
    expected = {
        "hadPrimarySource": str(
            extracted_primary_sources["confluence-vvt"].stableTargetId
        ),
        "identifierInPrimarySource": "DS-2023-177",
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "title": [
            {
                "value": "Accessing and increasing vaccine readiness in Sub-Saharan Africa "
                "(VRSA) – Work Package 1",  # noqa: RUF001
                "language": TextLanguage.EN,
            }
        ],
    }
    page_ids = fetch_all_data_page_ids()
    confluence_vvt_source_gens = tee(fetch_all_pages_data(page_ids), 2)

    ldap_authors = extract_confluence_vvt_authors(confluence_vvt_source_gens[0])
    ldap_author_gens = tee(ldap_authors, 2)

    merged_ids_by_query_string = get_merged_ids_by_query_string(
        ldap_author_gens[0], extracted_primary_sources["ldap"]
    )

    mex_activities = transform_confluence_vvt_sources_to_extracted_activities(
        confluence_vvt_source_gens[1],
        extracted_primary_sources["confluence-vvt"],
        merged_ids_by_query_string,
        unit_merged_ids_by_synonym,
    )

    mex_activity = next(mex_activities)

    assert mex_activity.model_dump(include=set(expected.keys())) == expected


def test_transform_confluence_vvt_source_items_to_mex_source_activity(
    monkeypatch: MonkeyPatch,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, Identifier],
    detail_page_data_json: dict[str, Any],
) -> None:
    response = Mock(spec=Response, status_code=200)
    response.json.return_value = detail_page_data_json

    # mocking create_session function
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response])

    monkeypatch.setattr(
        ConfluenceVvtConnector,
        "__init__",
        lambda self: setattr(self, "session", session),
    )
    page_ids = ["123456"]
    confluence_vvt_sources = fetch_all_pages_data(page_ids)

    fake_identifier = Identifier.generate()

    fake_authors = {
        "Test Verantwortliche 1": [fake_identifier],
        "Test mitarbeitende 1": [fake_identifier],
        "test ggfs vertreter": [fake_identifier],
    }

    confluence_primary_source = extracted_primary_sources["confluence-vvt"]

    mex_activities = list(
        transform_confluence_vvt_sources_to_extracted_activities(
            confluence_vvt_sources,
            confluence_primary_source,
            fake_authors,
            unit_merged_ids_by_synonym,
        )
    )

    mex_activity = mex_activities[0]

    expected = {
        "abstract": [
            {"value": "test description, test test test, test zwecke des vorhabens"}
        ],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "contact": [fake_identifier],
        "hadPrimarySource": confluence_primary_source.stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "001-002",
        "involvedPerson": [
            fake_identifier,
            fake_identifier,
            fake_identifier,
        ],
        "involvedUnit": [unit_merged_ids_by_synonym["FG99"]],
        "responsibleUnit": [unit_merged_ids_by_synonym["FG99"]],
        "stableTargetId": Joker(),
        "title": [{"value": "Test Title"}],
    }
    assert mex_activity.model_dump(exclude_defaults=True, exclude_none=True) == expected

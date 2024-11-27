from itertools import tee
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests.models import Response

from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.models import ExtractedActivity, ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
    TextLanguage,
)
from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.extract import (
    extract_confluence_vvt_authors,
    fetch_all_vvt_pages_ids,
    get_contact_from_page,
    get_involved_persons_from_page,
    get_all_persons_from_all_pages,
    get_page_data_by_id,
)
from mex.extractors.confluence_vvt.transform import (
    transform_confluence_vvt_activities_to_extracted_activities,
    transform_confluence_vvt_page_to_extracted_activity,
)
from mex.extractors.mapping.types import AnyMappingModel

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.mark.integration
def test_transform_confluence_vvt_page_to_extracted_activity(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    confluence_vvt_activity_mapping: AnyMappingModel,
) -> None:
    expected = {
        "hadPrimarySource": "dhVuI8dsGq7mwtc1xzj3be",
        "identifierInPrimarySource": "2023-177",
        "contact": ["gEKrcNH5xEQVSqn4UIrd4j"],
        "responsibleUnit": ["gEKrcNH5xEQVSqn4UIrd4j"],
        "title": Joker(),
        "abstract": Joker(),
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "documentation": Joker(),
        "involvedUnit": [
            "gEKrcNH5xEQVSqn4UIrd4j",
        ],
        "identifier": "eJGEaOXzHvJnfyyFKClEno",
        "stableTargetId": "cbkXBbmPvGb2GCUJcqoL8h",
    }
    connector = ConfluenceVvtConnector.get()
    page_data = connector.get_page_by_id("89780861")

    contacts = get_contact_from_page(page_data, confluence_vvt_activity_mapping)
    involved_persons = get_involved_persons_from_page(
        page_data, confluence_vvt_activity_mapping
    )

    ldap_authors = extract_confluence_vvt_authors(contacts + involved_persons)
    merged_ids_by_query_string = get_merged_ids_by_query_string(
        ldap_authors, extracted_primary_sources["ldap"]
    )

    extracted_activity = transform_confluence_vvt_page_to_extracted_activity(
        page_data,
        extracted_primary_sources["confluence-vvt"],
        confluence_vvt_activity_mapping,
        merged_ids_by_query_string,
        unit_merged_ids_by_synonym,
    )
    assert extracted_activity
    assert (
        extracted_activity.model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.integration
def test_transform_confluence_vvt_page_to_extracted_activities(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    confluence_vvt_activity_mapping: AnyMappingModel,
) -> None:
    all_pages = list(get_page_data_by_id(fetch_all_vvt_pages_ids()))

    all_persons = get_all_persons_from_all_pages(
        all_pages, confluence_vvt_activity_mapping
    )

    ldap_authors = extract_confluence_vvt_authors(all_persons)
    merged_ids_by_query_string = get_merged_ids_by_query_string(
        ldap_authors, extracted_primary_sources["ldap"]
    )
    extracted_activities = transform_confluence_vvt_activities_to_extracted_activities(
        all_pages,
        extracted_primary_sources["confluence-vvt"],
        confluence_vvt_activity_mapping,
        merged_ids_by_query_string,
        unit_merged_ids_by_synonym,
    )

    assert len(extracted_activities) == 18


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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.types import TextLanguage
from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.extract import (
    extract_confluence_vvt_authors,
    fetch_all_vvt_pages_ids,
    get_all_persons_from_all_pages,
    get_contact_from_page,
    get_involved_persons_from_page,
    get_page_data_by_id,
)
from mex.extractors.confluence_vvt.transform import (
    transform_confluence_vvt_activities_to_extracted_activities,
    transform_confluence_vvt_page_to_extracted_activity,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ActivityMapping

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.mark.integration
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_transform_confluence_vvt_page_to_extracted_activity(
    confluence_vvt_activity_mapping: ActivityMapping,
) -> None:
    expected = {
        "hadPrimarySource": "dhVuI8dsGq7mwtc1xzj3be",
        "identifierInPrimarySource": "2024-002",
        "contact": ["6rqNvZSApUHlz8GkkVP48"],
        "responsibleUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "title": [{"value": "Test Title"}],
        "abstract": [
            {"value": "test description, test test test", "language": TextLanguage.DE}
        ],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "documentation": [
            {
                "title": "Eintrag im Verzeichnis der Verarbeitungstätigkeiten des RKI",
                "url": "http://localhost:8050/v0/confluence_vvt/pages/viewpage.action?pageId=123457",
            }
        ],
        "involvedUnit": ["6rqNvZSApUHlz8GkkVP48", "cjna2jitPngp6yIV63cdi9"],
        "identifier": "kHcfyrJH1yCdwNJmtgm40",
        "stableTargetId": "e3z2u6QyEdSuGra5MhCTzM",
    }
    connector = ConfluenceVvtConnector.get()
    page_data = connector.get_page_by_id("123457")
    assert page_data is not None

    contacts = get_contact_from_page(page_data, confluence_vvt_activity_mapping)
    involved_persons = get_involved_persons_from_page(
        page_data, confluence_vvt_activity_mapping
    )

    ldap_authors = extract_confluence_vvt_authors(contacts + involved_persons)
    merged_ids_by_query_string = get_merged_ids_by_query_string(
        ldap_authors, get_extracted_primary_source_id_by_name("ldap")
    )

    extracted_activity = transform_confluence_vvt_page_to_extracted_activity(
        page_data,
        confluence_vvt_activity_mapping,
        merged_ids_by_query_string,
    )
    assert extracted_activity
    assert (
        extracted_activity.model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.integration
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_transform_confluence_vvt_page_to_extracted_activities(
    confluence_vvt_activity_mapping: ActivityMapping,
) -> None:
    all_pages = get_page_data_by_id(fetch_all_vvt_pages_ids())

    all_persons = get_all_persons_from_all_pages(
        all_pages, confluence_vvt_activity_mapping
    )

    ldap_authors = extract_confluence_vvt_authors(all_persons)
    merged_ids_by_query_string = get_merged_ids_by_query_string(
        ldap_authors, get_extracted_primary_source_id_by_name("ldap")
    )
    extracted_activities = transform_confluence_vvt_activities_to_extracted_activities(
        all_pages,
        confluence_vvt_activity_mapping,
        merged_ids_by_query_string,
    )

    assert len(extracted_activities) == 1

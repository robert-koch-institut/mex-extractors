from pathlib import Path

import pytest

from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.models import ActivityMapping
from mex.common.testing import Joker
from mex.common.types import MergedOrganizationalUnitIdentifier
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

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_transform_confluence_vvt_page_to_extracted_activity(
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    confluence_vvt_activity_mapping: ActivityMapping,
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
        unit_merged_ids_by_synonym,
    )
    assert extracted_activity
    assert (
        extracted_activity.model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_transform_confluence_vvt_page_to_extracted_activities(
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    confluence_vvt_activity_mapping: ActivityMapping,
) -> None:
    all_pages = list(get_page_data_by_id(fetch_all_vvt_pages_ids()))

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
        unit_merged_ids_by_synonym,
    )

    assert len(extracted_activities) >= 50

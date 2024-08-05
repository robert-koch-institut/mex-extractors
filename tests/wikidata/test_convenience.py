from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.common.models import ExtractedPrimarySource
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.common.wikidata.transform import (
    transform_wikidata_organization_to_extracted_organization,
)
from mex.wikidata import convenience
from mex.wikidata.convenience import (
    get_merged_organization_id_by_query_with_extract_transform_and_load,
)


@pytest.mark.usefixtures(
    "mocked_wikidata",
)
def test_get_merged_organization_id_by_query_with_extract_transform_and_load(
    wikidata_organization: WikidataOrganization,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    monkeypatch: MonkeyPatch,
) -> None:

    query_string = "Robert Koch-Institut"
    wikidata_primary_source = extracted_primary_sources["wikidata"]
    extracted_wikidata_organization = (
        transform_wikidata_organization_to_extracted_organization(
            wikidata_organization, wikidata_primary_source
        )
    )

    # mock all the things
    mocked_search_organization_by_label = Mock(side_effect=search_organization_by_label)
    monkeypatch.setattr(
        convenience, "search_organization_by_label", mocked_search_organization_by_label
    )
    mocked_transform_wikidata_organization_to_extracted_organization = Mock(
        side_effect=transform_wikidata_organization_to_extracted_organization
    )
    monkeypatch.setattr(
        convenience,
        "transform_wikidata_organization_to_extracted_organization",
        mocked_transform_wikidata_organization_to_extracted_organization,
    )
    mocked_load = Mock()
    monkeypatch.setattr(convenience, "load", mocked_load)

    # organization found and transformed
    returned = get_merged_organization_id_by_query_with_extract_transform_and_load(
        query_string, wikidata_primary_source
    )
    assert returned == extracted_wikidata_organization.stableTargetId
    mocked_search_organization_by_label.assert_called_once_with(query_string)
    mocked_transform_wikidata_organization_to_extracted_organization.assert_called_once_with(
        wikidata_organization, wikidata_primary_source
    )
    mocked_load.assert_called_once_with([extracted_wikidata_organization])

    # transformation returns no organization
    mocked_search_organization_by_label.reset_mock()
    mocked_transform_wikidata_organization_to_extracted_organization.side_effect = None
    mocked_transform_wikidata_organization_to_extracted_organization.return_value = None
    mocked_transform_wikidata_organization_to_extracted_organization.reset_mock()
    mocked_load.reset_mock()
    returned = get_merged_organization_id_by_query_with_extract_transform_and_load(
        query_string, wikidata_primary_source
    )
    assert returned is None
    mocked_search_organization_by_label.assert_called_once_with(query_string)
    mocked_transform_wikidata_organization_to_extracted_organization.assert_called_once_with(
        wikidata_organization, wikidata_primary_source
    )
    mocked_load.assert_not_called()

    # search returns no organization
    mocked_search_organization_by_label.side_effect = None
    mocked_search_organization_by_label.return_value = None
    mocked_search_organization_by_label.reset_mock()
    mocked_transform_wikidata_organization_to_extracted_organization.reset_mock()
    mocked_load.reset_mock()
    returned = get_merged_organization_id_by_query_with_extract_transform_and_load(
        query_string, wikidata_primary_source
    )
    assert returned is None
    mocked_search_organization_by_label.assert_called_once_with(query_string)
    mocked_transform_wikidata_organization_to_extracted_organization.assert_not_called()
    mocked_load.assert_not_called()

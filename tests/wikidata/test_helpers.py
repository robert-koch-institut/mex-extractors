from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.common.models import ExtractedLocation
from mex.common.primary_source.helpers import (
    get_extracted_primary_source_by_name,
)
from mex.common.wikidata.helpers import WikidataOrganizationResult
from mex.extractors.wikidata import helpers
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedOrganization


def test_get_wikidata_organization_by_id(
    monkeypatch: MonkeyPatch,
    wikidata_extracted_organization: ExtractedOrganization,
) -> None:
    """Load Wikidata dependencies and return only the organization."""
    primary_source = get_extracted_primary_source_by_name("wikidata")
    assert primary_source is not None

    mocked_location = ExtractedLocation(
        identifierInPrimarySource="Q183",
        hadPrimarySource=primary_source.stableTargetId,
        name=["Deutschland"],
    )

    mocked_organization = wikidata_extracted_organization.model_copy(
        update={
            "country": [mocked_location.stableTargetId],
        }
    )

    lookup_result = WikidataOrganizationResult(
        organization=mocked_organization,
        locations=[mocked_location],
        primary_source=primary_source,
    )

    mocked_lookup = Mock(return_value=lookup_result)
    mocked_load = Mock()

    monkeypatch.setattr(
        helpers,
        "get_wikidata_organization_with_locations",
        mocked_lookup,
    )
    monkeypatch.setattr(
        helpers,
        "load",
        mocked_load,
    )

    result = helpers.get_wikidata_organization_by_id("Q679041")

    assert result == mocked_organization
    assert result.country == [mocked_location.stableTargetId]

    mocked_lookup.assert_called_once_with("Q679041")
    mocked_load.assert_called_once_with(
        [primary_source, mocked_location, mocked_organization]
    )


@pytest.mark.usefixtures("mocked_wikidata")
def test_get_wikidata_extracted_organization_id_by_name(
    monkeypatch: MonkeyPatch,
    wikidata_extracted_organization: ExtractedOrganization,
) -> None:
    """Wikidata helper finds "Robert Koch-Institut"."""
    query_rki = "Robert Koch-Institut"

    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    returned = get_wikidata_extracted_organization_id_by_name(query_rki)
    mocked_load.assert_called_once()

    assert returned == wikidata_extracted_organization.stableTargetId


def test_get_wikidata_extracted_organization_id_by_name_for_nonsense_query() -> None:
    """Wikidata helper returns None for nonsense query."""
    query_nonsense = "this should not give a match"
    returned = get_wikidata_extracted_organization_id_by_name(query_nonsense)

    assert returned is None

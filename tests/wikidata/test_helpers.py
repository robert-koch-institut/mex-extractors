from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.extractors.wikidata import helpers
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedOrganization


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

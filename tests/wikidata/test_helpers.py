from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.wikidata import helpers
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
    get_wikidata_organization_by_id,
)


@pytest.mark.usefixtures("mocked_wikidata")
def test_get_wikidata_extracted_organization_id_by_name(
    monkeypatch: MonkeyPatch,
) -> None:
    """Wikidata helper finds "Robert Koch-Institut"."""
    get_wikidata_extracted_organization_id_by_name.cache_clear()
    get_wikidata_organization_by_id.cache_clear()
    query_rki = "Robert Koch-Institut"

    mocked_load = Mock()
    monkeypatch.setattr(helpers, "load", mocked_load)

    returned = get_wikidata_extracted_organization_id_by_name(query_rki)
    mocked_load.assert_called_once()

    assert returned == MergedOrganizationIdentifier("ga6xh6pgMwgq7DC7r6Wjqg")


@pytest.mark.integration
def test_get_wikidata_extracted_organization_id_by_name_for_nonsensequery() -> None:
    """Wikidata helper returns None for nonensense query."""
    get_wikidata_extracted_organization_id_by_name.cache_clear()

    query_nonsense = "this should not give a match"
    returned = get_wikidata_extracted_organization_id_by_name(query_nonsense)

    assert returned is None

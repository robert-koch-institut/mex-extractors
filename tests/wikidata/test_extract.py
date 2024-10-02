from unittest.mock import Mock

from pytest import MonkeyPatch

from mex.common.models import ExtractedPrimarySource
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)
from mex.extractors.wikidata import extract
from mex.extractors.wikidata.extract import (
    get_merged_organization_id_by_query_with_transform_and_load,
)


def test_get_organization_merged_id_by_query_with_transform_and_load(
    wikidata_organization: WikidataOrganization,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    monkeypatch: MonkeyPatch,
) -> None:
    wikidata_primary_source = extracted_primary_sources["wikidata"]

    mocked_load = Mock()
    monkeypatch.setattr(extract, "load", mocked_load)

    returned = get_merged_organization_id_by_query_with_transform_and_load(
        {"foo": wikidata_organization}, wikidata_primary_source
    )
    mocked_load.assert_called_once()
    mocked_load.call_args_list = [
        list(
            transform_wikidata_organizations_to_extracted_organizations(
                [wikidata_organization], wikidata_primary_source
            )
        )
    ]

    assert returned == {"foo": "ga6xh6pgMwgq7DC7r6Wjqg"}

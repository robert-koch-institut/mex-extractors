from mex.common.models import ExtractedPrimarySource
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)
from mex.extractors.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations_with_query,
)


def test_transform_wikidata_organizations_to_extracted_organizations_with_query(
    wikidata_organization: WikidataOrganization,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    expected = {
        "test": next(
            transform_wikidata_organizations_to_extracted_organizations(
                [wikidata_organization], extracted_primary_sources["wikidata"]
            )
        )
    }
    assert (
        transform_wikidata_organizations_to_extracted_organizations_with_query(
            {"test": wikidata_organization}, extracted_primary_sources["wikidata"]
        )
        == expected
    )

from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
)
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)


def transform_wikidata_organizations_to_extracted_organizations_with_query(
    wikidata_organizations_by_query: dict[str, WikidataOrganization],
    extracted_primary_source_wikidata: ExtractedPrimarySource,
) -> dict[str, ExtractedOrganization]:
    """Return a mapping from the search query to the Extracted Organizations.

    Args:
        wikidata_organizations_by_query: dictionary with string keys and
            WikidataOrganization values
        extracted_primary_source_wikidata: ExtractedPrimarySource for Wikidata
    Returns:
        Dict with keys: search query and values: Extracted Organization.
    """
    query_to_organization = {}
    for query, organization in wikidata_organizations_by_query.items():
        if extracted_organizations := list(
            transform_wikidata_organizations_to_extracted_organizations(
                [organization], extracted_primary_source_wikidata
            )
        ):
            query_to_organization[query] = extracted_organizations[0]
        else:
            continue
    return query_to_organization

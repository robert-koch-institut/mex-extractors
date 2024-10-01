from functools import cache

from mex.common.primary_source.helpers import get_extracted_primary_source_by_name
from mex.common.types import MergedOrganizationIdentifier
from mex.common.wikidata.convenience import get_extracted_organization_from_wikidata
from mex.extractors.sinks import load


@cache
def load_extracted_organization_and_return_stabletargetid(
    query_string: str,
) -> MergedOrganizationIdentifier | None:
    """Get extracted organization and load it and return its Id.

    Args:
        query_string: query string to search in wikidata

    Returns:
        ExtractedOrganization.stableTargetId if one matching organization is
           found in Wikidata lookup.
        None if multiple matches / no organization is found.
    """
    wikidata_primary_source = get_extracted_primary_source_by_name("wikidata")
    extracted_organization = get_extracted_organization_from_wikidata(
        query_string, wikidata_primary_source
    )
    if extracted_organization:
        load([extracted_organization])
        return extracted_organization.stableTargetId

    return None

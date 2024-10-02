from mex.common.models import ExtractedPrimarySource
from mex.common.types import MergedOrganizationIdentifier
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.extractors.sinks import load
from mex.extractors.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations_with_query,
)


def get_merged_organization_id_by_query_with_transform_and_load(
    wikidata_organizations_by_query: dict[str, WikidataOrganization],
    wikidata_primary_source: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Return a mapping from organizations to their stable target ID.

    WikidataOrganizations are transformed into ExtractedOrganizations and loaded into
    the configured sink.

    Args:
        wikidata_organizations_by_query: dict of Extracted organizations by query string
        wikidata_primary_source: Primary source item for wikidata

    Returns:
        Dict with organization label keys and stable target ID values
    """
    extracted_organizations_by_query = (
        transform_wikidata_organizations_to_extracted_organizations_with_query(
            wikidata_organizations_by_query, wikidata_primary_source
        )
    )
    load(extracted_organizations_by_query.values())

    return {
        query: MergedOrganizationIdentifier(organization.stableTargetId)
        for query, organization in extracted_organizations_by_query.items()
    }

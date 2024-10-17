from mex.common.primary_source.helpers import (
    get_extracted_primary_source_by_name,
)
from mex.common.types import MergedOrganizationIdentifier, MergedPrimarySourceIdentifier
from mex.common.wikidata.helpers import get_extracted_organization_from_wikidata
from mex.extractors.sinks import load


# Helper for primary source
def get_extracted_primary_source_id_by_name(
    name: str,
) -> MergedPrimarySourceIdentifier | None:
    """Use helper function to look up a primary source and return its stableTargetId.

    A primary source is searched by its name and loaded into the configured sink.
    Also it's stable target id is returned.

    Returns:
        ExtractedPrimarySource stableTargetId if one matching primary source is found.
        None if multiple matches / no match is found
    """
    extracted_primary_source = get_extracted_primary_source_by_name(name)

    if extracted_primary_source is None:
        return None

    load([extracted_primary_source])

    return extracted_primary_source.stableTargetId


# Helper for Wikidata
def get_wikidata_extracted_organization_id_by_name(
    name: str,
) -> MergedOrganizationIdentifier | None:
    """Use helper function to look up an organization and return its stableTargetId.

    An organization searched by its name on Wikidata and loaded into the configured
    sink. Also it's stable target id is returned.

    Returns:
        ExtractedOrganization stableTargetId if one matching organization is found.
        None if multiple matches / no match is found
    """
    extracted_organization = get_extracted_organization_from_wikidata(name)

    if extracted_organization is None:
        return None

    load([extracted_organization])

    return extracted_organization.stableTargetId

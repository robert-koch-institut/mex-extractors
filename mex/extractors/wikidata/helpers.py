from functools import cache

from mex.common.exceptions import MExError
from mex.common.types import MergedOrganizationIdentifier
from mex.common.wikidata.helpers import get_extracted_organization_from_wikidata
from mex.extractors.primary_source.helpers import load_extracted_primary_source_by_name
from mex.extractors.sinks import load


@cache
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
    wikidata_primary_source = load_extracted_primary_source_by_name("wikidata")
    if not wikidata_primary_source:
        msg = "Primary source for wikidata not found"
        raise MExError(msg)

    extracted_organization = get_extracted_organization_from_wikidata(
        name, wikidata_primary_source
    )

    if extracted_organization is None:
        return None

    load([extracted_organization])

    return extracted_organization.stableTargetId

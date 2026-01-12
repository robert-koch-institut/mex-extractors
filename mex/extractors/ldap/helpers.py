from collections.abc import Iterable

from mex.common.exceptions import EmptySearchResultError, FoundMoreThanOneError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_any_ldap_actor_to_extracted_persons_or_contact_points,
)
from mex.common.models import ExtractedOrganizationalUnit
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import get_wikidata_organization_by_id


def get_ldap_merged_id_by_query(
    query: str,
    extracted_organizational_units: Iterable[ExtractedOrganizationalUnit],
    limit: int = 10,
) -> MergedPersonIdentifier | MergedContactPointIdentifier:
    """Extract, transform and load ldap person or contact and return merged ID.

    Args:
        query: person or functional account query
        extracted_organizational_units: extracted organizational units
        limit: How many items to return

    Raises:
        EmptySearchResultError if no result, FoundMoreThanOneError if multiple results

    Returns:
        merged person or contact point
    """
    connector = LDAPConnector.get()
    ldap_actors = connector.get_persons_or_functional_accounts(query=query, limit=limit)
    rki_organization = get_wikidata_organization_by_id("RKI")
    if not rki_organization:
        msg = "RKI Organization not found in wikidata."
        raise ValueError(msg)
    extracted_persons_or_contact_points = (
        transform_any_ldap_actor_to_extracted_persons_or_contact_points(
            ldap_actors,
            extracted_organizational_units,
            get_extracted_primary_source_id_by_name("ldap"),
            rki_organization,
        )
    )
    if len(extracted_persons_or_contact_points) == 0:
        msg = f"No result for query: {query}"
        raise EmptySearchResultError(msg)
    if len(extracted_persons_or_contact_points) > 1:
        msg = f"More than one result for query: {query}"
        raise FoundMoreThanOneError(msg)
    load(extracted_persons_or_contact_points)
    return extracted_persons_or_contact_points[0].stableTargetId

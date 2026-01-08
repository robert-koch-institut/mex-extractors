from collections.abc import Iterable

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


def get_ldap_merged_ids_by_query(
    query: str,
    extracted_organizational_units: Iterable[ExtractedOrganizationalUnit],
    limit: int = 10,
) -> list[MergedPersonIdentifier | MergedContactPointIdentifier]:
    """Extract, transform and load ldap person or contact and return merged ID.

    Args:
        query: person or functional account query
        extracted_organizational_units: extracted organizational units
        limit: How many items to return

    Returns:
        list of merged person or contact point ids
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
    load(extracted_persons_or_contact_points)
    return [item.stableTargetId for item in extracted_persons_or_contact_points]

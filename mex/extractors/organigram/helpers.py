from collections.abc import Iterable
from functools import lru_cache

from mex.common.exceptions import EmptySearchResultError, MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.helpers import get_ldap_units_for_employee_ids
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.logging import watch_progress
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


@lru_cache(maxsize=1)
def _get_cached_unit_merged_ids_by_synonyms() -> dict[
    str, list[MergedOrganizationalUnitIdentifier]
]:
    """Extract, transform and load the organigram, then group unit IDs by synonym.

    Returns:
        Lookup of organizational unit identifiers by synonym
    """
    rki_organization_id = get_wikidata_extracted_organization_id_by_name("RKI")
    if not rki_organization_id:
        msg = "RKI wikidata organization not found"
        raise EmptySearchResultError(msg)
    organigram_units = extract_organigram_units()
    extracted_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        get_extracted_primary_source_id_by_name("organigram"),
        rki_organization_id,
    )
    load(extracted_organizational_units)
    return get_unit_merged_ids_by_synonyms(extracted_organizational_units)


def get_unit_merged_id_by_synonym(
    synonym: str,
) -> list[MergedOrganizationalUnitIdentifier] | None:
    """Search a merged organizational unit id by synonym.

    Args:
        synonym: synonym of organizational unit

    Returns:
        list of merged organizational unit ids if found else None
    """
    unit_merged_ids_by_synonyms = _get_cached_unit_merged_ids_by_synonyms()
    return unit_merged_ids_by_synonyms.get(synonym, None)


def resolve_organizational_unit_with_fallback(
    extracted_unit: str | None,
    contact_employee_ids: list[str],
    involved_employee_ids: list[str],
) -> list[MergedOrganizationalUnitIdentifier] | None:
    """."""
    units_by_synonym = _get_cached_unit_merged_ids_by_synonyms()

    if extracted_unit:
        unit_ids = units_by_synonym.get(extracted_unit)
        if unit_ids:
            return unit_ids

    employee_ids = set(contact_employee_ids) | set(involved_employee_ids)
    if not employee_ids:
        return None

    ldap_units = get_ldap_units_for_employee_ids(employee_ids)

    for ldap_unit in ldap_units:
        unit_ids = units_by_synonym.get(ldap_unit)
        if unit_ids:
            return unit_ids

    return None

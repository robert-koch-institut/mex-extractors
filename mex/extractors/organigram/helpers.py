from collections.abc import Iterable
from functools import lru_cache

from mex.common.exceptions import EmptySearchResultError, MExError
from mex.common.ldap.connector import LDAPConnector
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
from mex.extractors.wikidata.helpers import get_wikidata_organization_by_id


@lru_cache(maxsize=1)
def _get_cached_unit_merged_ids_by_synonyms() -> dict[
    str, list[MergedOrganizationalUnitIdentifier]
]:
    """Extract, transform and load the organigram, then group unit IDs by synonym.

    Returns:
        Lookup of organizational unit identifiers by synonym
    """
    rki_organization = get_wikidata_organization_by_id("RKI")
    if not rki_organization:
        msg = "RKI wikidata organization not found"
        raise EmptySearchResultError(msg)
    organigram_units = extract_organigram_units()
    extracted_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        get_extracted_primary_source_id_by_name("organigram"),
        rki_organization,
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

def get_ldap_units_for_employee_ids(employee_ids: Iterable[str]) -> dict[str, str]:
    """."""
    ldap = LDAPConnector.get()
    result = {}

    for employee_id in watch_progress(employee_ids, "get_ldap_units_for_employee_ids"):
        if not employee_id:
            continue

        try:
            person = ldap.get_person(employee_id=employee_id)
        except MExError:
            continue

        if person.organizational_unit:
            result[employee_id] = person.organizational_unit

    return result

def match_extracted_unit_with_organigram_units(extracted_unit: str)->bool:
    # rki_organization = get_wikidata_organization_by_id("RKI")
    # if not rki_organization:
    #     msg = "RKI wikidata organization not found"
    #     raise EmptySearchResultError(msg)
    organigram_units = extract_organigram_units()
    # extracted_organizational_units = transform_organigram_units_to_organizational_units(
    #     organigram_units,
    #     get_extracted_primary_source_id_by_name("organigram"),
    #     rki_organization,
    # )
    #load(extracted_organizational_units)
    #exists = any(extracted_organizational_unit.identifier == extracted_unit for extracted_organizational_unit in extracted_organizational_units)
    # extracted_organizational_units[0]= ExtractedOrganizationalUnit(hadPrimarySource=MergedPrimarySourceIdentifier("dsnYIq1AxYMLcTbSIBvDSs"), identifierInPrimarySource='praes', parentUnit=None, name=[Text(value='Institutsleitung', language=<TextLanguage.DE: 'de'>), Text(value='President', language=<TextLanguage.EN: 'en'>)], alternativeName=[Text(value='Praes', language=<TextLanguage.DE: 'de'>), Text(value='PrÃ¤s', language=<TextLanguage.DE: 'de'>)], email=[], shortName=[], unitOf=[MergedOrganizationIdentifier("ga6xh6pgMwgq7DC7r6Wjqg")], website=[Link(language=<LinkLanguage.DE: 'de'>, title='Institutsleitung auf rki.de', url='https://www.rki.de/DE/Institut/Organisation/Leitung/leitung-node.html')], entityType='ExtractedOrganizationalUnit', identifier=ExtractedOrganizationalUnitIdentifier("gFgzD2U7HjFF4bO8WbqkM0"), stableTargetId=MergedOrganizationalUnitIdentifier("hIbCmDVGw4ETuumjF4HBVk"))
    #identifiers = [ u.identifier for u in  extracted_organizational_units]
    # identifiers = [ExtractedOrganizationalUnitIdentifier("cmIwNTUG7lXnzglIePDwMU")]
    # TODO match extracted_unit with organigram unit identifiers 

    organigram_units_identifiers = [unit.identifier for unit in organigram_units]
    return extracted_unit in organigram_units_identifiers






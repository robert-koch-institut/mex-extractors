from typing import TYPE_CHECKING

from mex.common.ldap.transform import analyse_person_string
from mex.extractors.datscha_web.connector import DatschaWebConnector
from mex.extractors.ldap.helpers import get_ldap_merged_person_id_by_query
from mex.extractors.logging import watch_progress
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.common.types import MergedOrganizationIdentifier, MergedPersonIdentifier
    from mex.extractors.datscha_web.models.item import DatschaWebItem


def extract_datscha_web_items() -> list[DatschaWebItem]:
    """Load datscha source items by scraping datscha-web pages.

    Returns:
        List of datscha web items
    """
    connector = DatschaWebConnector.get()
    items = []
    for item_url in watch_progress(
        connector.get_item_urls(), "extract_datscha_web_items"
    ):
        item = connector.get_item(item_url)
        items.append(item)
    return items


def extract_datscha_web_source_contact_ids_by_query(
    datscha_web_items: Iterable[DatschaWebItem],
) -> dict[str, list[MergedPersonIdentifier]]:
    """Extract LDAP persons with their query string for datscha-web source contacts.

    Args:
        datscha_web_items: Datscha-web items

    Returns:
        List of LDAP persons with query
    """
    seen = set()
    merged_person_ids_by_query: dict[str, list[MergedPersonIdentifier]] = {}
    for source in watch_progress(
        datscha_web_items, "extract_datscha_web_source_contacts"
    ):
        names = source.auskunftsperson
        if names is None or names in seen:
            continue
        seen.add(names)
        collected_ids = [
            person_id
            for name in analyse_person_string(names)
            if (
                person_id := get_ldap_merged_person_id_by_query(
                    surname=name.surname, given_name=name.given_name
                )
            )
        ]
        merged_person_ids_by_query[names] = collected_ids
    return merged_person_ids_by_query


def extract_datscha_web_organizations(
    datscha_web_items: Iterable[DatschaWebItem],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        datscha_web_items: Iterable of DatschaWebItem

    Returns:
        Dict with keys DatschaWebItem.Auftragsverarbeiter,
            DatschaWebItem.Empfaenger_der_Daten_im_Drittstaat, and
            DatschaWebItem.Empfaenger_der_verarbeiteten_uebermittelten_oder_offengelegten_Daten,
            and values: MergedOrganizationIdentifier
    """
    partner_to_org_map = {}
    for item in datscha_web_items:
        for partner in item.get_partners():
            if (
                partner
                and partner != "None"
                and (
                    organization := get_wikidata_extracted_organization_id_by_name(
                        partner
                    )
                )
            ):
                partner_to_org_map[partner] = organization
    return partner_to_org_map

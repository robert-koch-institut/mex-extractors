from collections.abc import Generator, Iterable

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import watch
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.datscha_web.connector import DatschaWebConnector
from mex.extractors.datscha_web.models.item import DatschaWebItem
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


@watch()
def extract_datscha_web_items() -> Generator[DatschaWebItem, None, None]:
    """Load datscha source items by scraping datscha-web pages.

    Returns:
        Generator for datscha web items
    """
    connector = DatschaWebConnector.get()
    for item_url in connector.get_item_urls():
        item = connector.get_item(item_url)
        yield item


@watch()
def extract_datscha_web_source_contacts(
    datscha_web_items: Iterable[DatschaWebItem],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons with their query string for datscha-web source contacts.

    Args:
        datscha_web_items: Datscha-web items

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in datscha_web_items:
        names = source.auskunftsperson
        if not names:
            continue
        if names in seen:
            continue
        seen.add(names)
        for name in analyse_person_string(names):
            persons = ldap.get_persons(
                surname=name.surname, given_name=name.given_name, limit=2
            )
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=names)


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
